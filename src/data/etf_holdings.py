import logging
import os
import datetime
from pathlib import Path
from tqdm import tqdm
import pandas as pd


ETF_CACHE_DIR=Path(os.getcwd()).parent.parent/'data'/'equity_market'/'1_etf'
print(ETF_CACHE_DIR)
if not os.path.exists(ETF_CACHE_DIR):
    print('Creating ETF cache directory')
    os.makedirs(ETF_CACHE_DIR)


base_url = 'https://www.ishares.com/us/products'
ishares_etf_url_spec = {
    'ivv': ['239726','ishares-core-sp-500-etf','IVV_holdings'],
    'iwm': ['239710','ishares-russell-2000-etf','IWM_holdings'],
    'iwb': ['239707','ishares-russell-1000-etf','IWB_holdings'],
    'iwb': ['239707','ishares-russell-1000-etf','IWB_holdings'],
    'ivw': ['239725','ishares-sp-500-growth-etf','IVW_holdings'],
    'iwo': ['239709','ishares-russell-2000-growth-etf','IWO_holdings'],
    'iwn': ['239712','ishares-russell-2000-value-etf','IWN_holdings'],
    'iwd': ['239708','ishares-russell-1000-value-etf','IWD_holdings'],
    'iwf': ['239706','ishares-russell-1000-growth-etf','IWF_holdings'],
    'ijr': ['239774','ishares-core-sp-smallcap-etf','IJR_holdings'],
    'qual': ['256101','ishares-msci-usa-quality-factor-etf','QUAL_holdings'],
    'usmv': ['239695','ishares-msci-usa-minimum-volatility-etf','USMV_holdings'],
    'iqlt': ['271540','ishares-msci-international-developed-quality-factor-etf','IQLT_holdings'],
    'hdv': ['239563','ishares-high-dividend-etf','HDV_holdings'],
    'iyr': ['239520','ishares-us-real-estate-etf','IYR_holdings']
}


def get_ishares_url(base_url, etf_id, etf_name, filename):
    query=f"fileType=csv&fileName={filename}&dataType=fund"
    ishares_url = f"{base_url}/{etf_id}/{etf_name}/1467271812596.ajax?{query}"
    return ishares_url


def cache_etf_holdings(tic, spec):
    etf_id, etf_name, filename = spec
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
    )
    return holdings


def cache_all_etf():
    etf_dfs = []
    for tic, spec in tqdm(ishares_etf_url_spec.items()):
        try:
            etf_dfs.append(cache_etf_holdings(tic,spec))
        except:
            logging.CRITICAL(f"Failed to cache ETF holdings for {tic}")
    etf_dfs = pd.concat(etf_dfs, sort=True)
    as_of_date = etf_dfs.as_of_date.values[0]
    etf_dfs.to_parquet(ETF_CACHE_DIR/f'ishares_holdings_{as_of_date}.parquet')
    etf_dfs.to_csv(ETF_CACHE_DIR/f'ishares_holdings_{as_of_date}.csv',index=False)

    return etf_dfs


if __name__ == '__main__':

    etf_holdings_df = cache_all_etf()