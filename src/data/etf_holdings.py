import logging
import os
import datetime
from pathlib import Path
from tqdm import tqdm
import pandas as pd


ETF_CACHE_DIR=Path(os.getcwd()).parent.parent/'data'/'equity_market'/'1_etf'
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


def cache_etf_holdings(spec):
    tic, etf_id, etf_name, filename = spec
    holdings_url = get_ishares_url(base_url, etf_id, etf_name, filename)
    etf_display_name = pd.read_csv(holdings_url, nrows=1, header=None).iloc[0, 0]
    as_of_date_str = pd.read_csv(holdings_url, skiprows=1, nrows=2, header=None).iloc[0, 1]
    as_of_date = pd.to_datetime(as_of_date_str).date().isoformat()
    holdings = (
        pd.read_csv(holdings_url, skiprows=9)
        .assign(etf_name=etf_display_name)
        .assign(etf_ticker=tic)
        .assign(as_of_date=as_of_date)
        .assign(Price=lambda x: x['Price'].astype(str))
        .dropna(subset=['Market Value'])
    )
    return holdings


def cache_etf_by_group(etf_url_meta_file):
    etf_url_df=pd.read_csv(ETF_META_DIR/etf_url_meta_file)
    etf_dfs = []
    for i, row in tqdm(etf_url_df.iterrows(), total=etf_url_df.shape[0], desc=f'Caching {etf_url_meta_file}'):
        try:
            spec = (row['ticker'], row['etf-id'], row['etf_name'], row['file_name'])
            etf_dfs.append(cache_etf_holdings(spec))
        except:
            logging.CRITICAL(f"Failed to cache ETF holdings for {etf_url_meta_file}")
    etf_dfs = pd.concat(etf_dfs, sort=True)
    etf_dfs.dropna(subset=['Asset Name'], inplace=True)
    return etf_dfs


def cache_all_etf():
    etf_dfs = []
    for f in os.listdir(ETF_META_DIR):
        etf_dfs.append(cache_etf_by_group(f))
    etf_dfs = pd.concat(etf_dfs)
    as_of_date = etf_dfs.as_of_date.values[0]
    etf_dfs.to_parquet(ETF_CACHE_DIR/f'ishares_holdings_{as_of_date}.parquet')
    etf_dfs.to_csv(ETF_CACHE_DIR/f'ishares_holdings_{as_of_date}.csv', index=False)

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


if __name__ == '__main__':

    etf_holdings_df = cache_all_etf()
    compile_etf_holdings()