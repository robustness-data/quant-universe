import logging
logger = logging.getLogger(__name__)
import os
import re
from datetime import datetime
import time
import random
from pathlib import Path
from tqdm import tqdm
import pandas as pd
from io import StringIO
import requests


ETF_CACHE_DIR=Path(os.getcwd()).parent.parent/'data'/'equity_market'/'1_ishares_etf'
ETF_META_DIR=ETF_CACHE_DIR/'meta'
print(ETF_CACHE_DIR)
if not os.path.exists(ETF_CACHE_DIR):
    print('Creating ETF cache directory')
    os.makedirs(ETF_CACHE_DIR)


base_url = 'https://www.ishares.com/us/products'


def get_ishares_url(base_url, etf_id, etf_name, filename):
    query=f"fileType=csv&fileName={filename}&dataType=fund"
    ishares_url = f"{base_url}/{etf_id}/{etf_name}/1467271812596.ajax?{query}"
    return ishares_url


def cache_etf_holdings(spec, N=1):
    tic, etf_id, etf_name, filename = spec
    holdings_url = get_ishares_url(base_url, etf_id, etf_name, filename)
    holdings_file = None
    n = 1
    while holdings_file is None:
        try:
            holdings_file = _download_etf_holdings(holdings_url)
        except Exception as e:
            print(f"Error: {e}")

        if holdings_file is None:
            print(f"Failed to download ETF holdings for {etf_name}. Retry {n}/{N}")
            n += 1
            time.sleep(random.randint(1, 3))
            if n > N:
                return None
        else:
            holdings_file['etf_ticker'] = tic
            holdings_file['etf_name'] = etf_name
            return holdings_file


def cache_etf_by_group(etf_url_meta_file):
    etf_url_df=pd.read_csv(ETF_META_DIR/etf_url_meta_file)
    etf_dfs = []
    for i, row in tqdm(etf_url_df.iterrows(), total=etf_url_df.shape[0], desc=f'Caching {etf_url_meta_file}'):
        spec = (row['ticker'], row['etf-id'], row['etf_name'], row['file_name'])
        holdings_file = cache_etf_holdings(spec)
        if holdings_file is not None:
            etf_dfs.append(holdings_file)
        else:
            print(f"Failed to cache ETF holdings for {row['etf_name']}")
    etf_dfs = pd.concat(etf_dfs, sort=True)
    return etf_dfs


def _find_ivv_url_spec():
    # use IVV as the reference
    etf_url_df=pd.read_csv(ETF_META_DIR/'ishares_core_urls.csv')
    ivv_spec=etf_url_df.query("ticker=='ivv'").iloc[0]
    return ivv_spec['ticker'], ivv_spec['etf-id'], ivv_spec['etf_name'], ivv_spec['file_name']


def _find_latest_date():
    spec = _find_ivv_url_spec()
    df = cache_etf_holdings(spec)
    return df.as_of_date.iloc[0]


def cache_all_etf(chunk_size=100):

    # get the cached ETF info
    last_date = _find_latest_date()
    filename = f'ishares_holdings_{last_date}.csv'
    has_cache = False
    if os.path.exists(ETF_CACHE_DIR/filename):
        print(f"ETF holdings for {last_date} already cached")
        cached_df = pd.read_csv(ETF_CACHE_DIR/filename)
        cached_etf_id_list = cached_df['etf_ticker'].apply(lambda x: x.lower()).unique().tolist()
        print(f"Found {len(cached_etf_id_list)} ETFs cached and the last date is {last_date}")
        has_cache = True
    else:
        cached_etf_id_list = []

    # get all the ETF urls info
    etf_meta = []
    for f in os.listdir(ETF_META_DIR):
        etf_meta.append(pd.read_csv(ETF_META_DIR/f))
    etf_meta = pd.concat(etf_meta)
    etf_meta.drop_duplicates(inplace=True)

    # request holdings for non-cached ETFs
    etf_dfs=[]
    count = 1
    for i, row in tqdm(etf_meta.iterrows(), total=etf_meta.shape[0], desc=f'Caching ETFs'):
        if row['ticker'] not in cached_etf_id_list:
            spec = (row['ticker'], row['etf-id'], row['etf_name'], row['file_name'])
            holdings_file = cache_etf_holdings(spec)
            if holdings_file is not None:
                etf_dfs.append(holdings_file)
            if count == chunk_size:
                break
            count += 1  # add one more count if successfully cached a new ETF

    if etf_dfs == []:
        return cached_df

    # compile the cache file
    etf_dfs = pd.concat(etf_dfs)
    if has_cache:
        etf_dfs = pd.concat([etf_dfs, cached_df])
        #etf_dfs = etf_dfs[etf_dfs['Market Value']!='Market Value']

    all_dates = etf_dfs.as_of_date.unique().tolist()
    if len(all_dates) > 1:
        print(f"Found more than one date in the ETF holdings: {all_dates}.")
        as_of_date=max(pd.to_datetime(all_dates)).isoformat()
        print(f"Using the latest date: {as_of_date} as the cached file name.")
    else:
        as_of_date = all_dates[0]

    try:
        etf_dfs.to_csv(ETF_CACHE_DIR/f'ishares_holdings_{as_of_date}.csv', index=False)
    except:
        print(f"Failed to save csv file for {as_of_date}")

    try:
        for c in ['Market Value', 'Weight (%)', 'Notional Value', 'Shares', 'Price', 'FX Rate']:
            etf_dfs[c] = etf_dfs[c].apply(_convert_to_float)
        etf_dfs.to_parquet(ETF_CACHE_DIR/f'ishares_holdings_{as_of_date}.parquet')
    except:
        print(f"Failed to save parquet file for {as_of_date}")

    return etf_dfs


# compile all ETF holdings parquet file and csv files into one, separately
def compile_etf_holdings():
    files=os.listdir(ETF_CACHE_DIR)
    parquet_files=[f for f in files if '.parquet' in f]
    csv_files=[f for f in files if '.csv' in f]
    parquet_files.sort()
    csv_files.sort()
    etf_dfs = []
    for f in parquet_files:
        etf_dfs.append(pd.read_parquet(ETF_CACHE_DIR/f))
    etf_dfs = pd.concat(etf_dfs, sort=True)
    etf_dfs.to_parquet(ETF_CACHE_DIR/f'ishares_holdings.parquet')
    etf_dfs.to_csv(ETF_CACHE_DIR/f'ishares_holdings.csv', index=False)

    return etf_dfs


def _download_etf_holdings(holdings_url):
    holdings_file = _fetch_ishares_holdings_file(holdings_url)
    start_line = None
    end_line = None
    if holdings_file is not None:
        for i, line in enumerate(holdings_file.splitlines()):
            if _extract_as_of_date(line):
                as_of_date = _extract_as_of_date(line).date().isoformat()
            if _extract_inception_date(line):
                inception_date = _extract_inception_date(line).date().isoformat()
            if _extract_shares_outstanding(line):
                shares_outstanding = _extract_shares_outstanding(line)
            if 'Ticker,Name,Sector' in line:
                start_line = i
            if 'The content contained herein' in line:
                end_line = i - 1
                break
    else:
        print(f"Error happened when requesting file from {holdings_url}")
        return None

    # Convert table to DataFrame
    if start_line is not None:
        table_text = '\n'.join(holdings_file.splitlines()[start_line:end_line])
        df = pd.read_csv(StringIO(table_text))
        df['as_of_date'] = as_of_date
        df['inception_date'] = inception_date
        df['shares_outstanding'] = shares_outstanding
        return df
    else:
        return None


def _fetch_ishares_holdings_file(holdings_url):
    r = requests.get(holdings_url)
    if r.status_code == 200:
        return r.text
    else:
        print(f'An error has occurred when fetching: {holdings_url}.')
        return None


def _extract_as_of_date(text):
    contextual_date_pattern = r'Fund Holdings as of,"(\w{3} \d{2}, \d{4})"'
    contextual_date_match = re.search(contextual_date_pattern, text)
    if contextual_date_match:
        contextual_date_str = contextual_date_match.group(1)
        contextual_date_obj = datetime.strptime(contextual_date_str, '%b %d, %Y')
    else:
        contextual_date_obj = None

    return contextual_date_obj

def _extract_inception_date(text):
    contextual_date_pattern = r'Inception Date,"(\w{3} \d{2}, \d{4})"'
    contextual_date_match = re.search(contextual_date_pattern, text)
    if contextual_date_match:
        contextual_date_str = contextual_date_match.group(1)
        contextual_date_obj = datetime.strptime(contextual_date_str, '%b %d, %Y')
    else:
        contextual_date_obj = None

    return contextual_date_obj


def _extract_shares_outstanding(text):
    match = re.search(r'Shares Outstanding,"([\d,]+)', text)
    if match:
        shares_outstanding = match.group(1).replace(',', '')
        return int(shares_outstanding)
    else:
        return None


def _convert_to_float(x):
    if isinstance(x, float):
        return x
    elif isinstance(x, int):
        return float(x)
    elif isinstance(x, str):
        try:
            return float(x.replace(',',''))
        except Exception as e:
            print(f'Failed to convert {x} to float: {e}')
            return x


if __name__ == '__main__':

    for i in range(3):
        cache_all_etf(chunk_size=100)
