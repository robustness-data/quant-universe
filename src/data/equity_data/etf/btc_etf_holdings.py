import sys, os
import itertools
from pathlib import Path
ROOT_DIR=Path(__file__).parent.parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.data.equity_data.tradingview import TradingView
from src.data.equity_data.yfinance_old import Stock
from src.config import DB_DIR
from src.data.equity_data.etf.holdings import get_ark_etf_holdings, scrape_webpage, spdr_etfs_urls
from src.script.compile_etf_holdings import _download_etf_holdings

import streamlit as st
import plotly.express as px
import sqlite3
import datetime
import pandas as pd
import numpy as np


holdings_urls = {
    "ARKB": "https://ark-funds.com/wp-content/uploads/funds-etf-csv/ARK_21SHARES_BITCOIN_ETF_ARKB_HOLDINGS.csv",
    'IBIT': "https://www.ishares.com/us/products/333011/fund/1467271812596.ajax?fileType=csv&fileName=IBIT_holdings&dataType=fund",
    "BRRR": "https://valkyrieinvest.com/brrr",
    "FBTC": "https://research2.fidelity.com/fidelity/screeners/etf/etfholdings.asp?symbol=FBTC&view=Holdings",
    "EZBC": "https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/39639/SINGLCLASS/franklin-bitcoin-etf/EZBC"
}


# Spot BTC ETFs
summary_urls ={
    "BTCW": [],
    "BTCO": [
        "https://www.invesco.com/us/financial-products/etfs/product-detail?ticker=BTCO"
    ],
    "FBTC": [
        "https://digital.fidelity.com/prgw/digital/research/quote/dashboard/key-statistics?symbol=FBTC",
        "https://digital.fidelity.com/prgw/digital/research/quote/dashboard/summary?symbol=FBTC"
    ],
}

def scrape_ibit_holdings():
    ibit_holdings = _download_etf_holdings(holdings_urls['IBIT'])\
        .set_index('Ticker')\
        .assign(Shares = lambda x: x['Shares'].apply(lambda y: y.replace(',','')).astype(float))\
        .rename(columns={'Market Value': 'mv'})\
        .assign(mv = lambda x: x['mv'].apply(lambda y: y.replace(',','')).astype(float))
    
    return pd.DataFrame({
        'IBIT': 
        {
            'date': ibit_holdings.xs('BTC')['as_of_date'],
            'btc_holdings': ibit_holdings.xs('BTC').Shares,
            'btc_mv': ibit_holdings.xs('BTC')['mv']/1e9,
            'average_mv': ibit_holdings.xs('BTC')['mv']/ibit_holdings.xs('BTC').Shares, 
            'cash_holdings': ibit_holdings.xs('USD').Shares
        }
    })


def scrape_arkb_holdings():
    arkb_holdings = get_ark_etf_holdings('ARKB').set_index('ticker')
    return pd.DataFrame({
        'ARKB': {
            'date': arkb_holdings.xs('BTC')['date'],
            'btc_holdings': arkb_holdings.xs('BTC')['shares'],
            'btc_mv': arkb_holdings.xs('BTC')['market_value']/1e9,
            'average_mv': arkb_holdings.xs('BTC').market_value/arkb_holdings.xs('BTC').shares,
            'cash_holdings': 0.0,
        }
    })


def scrape_brrr_holdings():
    brrr_holdings_soup = scrape_webpage(holdings_urls['BRRR'])
    divs = brrr_holdings_soup.find_all('div')
    for div in divs:
        if div.has_attr('class'):
            if "mcb-item-jm3z2by6" in div['class']:
                tables = div.find_all('table')
                parsed_holdings = pd.read_html(str(tables[0]))[0]
                
            if "mcb-column-inner-118575426" in div['class']:
                h2s = div.find_all('h2')
                for h2 in h2s:
                    if h2.text == "Holdings":
                        p = h2.find_next_sibling('p')
                        date = p.text.split(' ')[-1]
                        date = pd.to_datetime(date).date().isoformat()

    brrr_info = {
        'btc_holdings': float(parsed_holdings.iloc[1,2].replace(',','')),
        'btc_mv': float(parsed_holdings.iloc[1,3])/1e9,
        'date': date,
        'cash_holdings': float(parsed_holdings.iloc[2,2].replace(',','')),
    }
    brrr_info['average_mv'] = 1e9*brrr_info['btc_mv']/brrr_info['btc_holdings']
    return pd.DataFrame({'BRRR': brrr_info})


def scrape_btc_etf_holdings(max_retry: int):
    scraper_map = {
        'ARKB': scrape_arkb_holdings,
        'IBIT': scrape_ibit_holdings,
        'BRRR': scrape_brrr_holdings
    }
    
    holdings_list = []
    for tic, func in scraper_map.items():
        retry_count = 0
        while retry_count < max_retry:
            try:
                holdings = func()
                holdings_list.append(holdings)
                retry_count = max_retry
                print(f"Successfully scraped {tic} holdings")
            except:
                retry_count += 1
                print(f"Failed to scrape {tic} holdings. Retrying...")
                continue
    return pd.concat(holdings_list, axis=1)


def write_to_db(btc_etf_holdings: pd.DataFrame):
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    btc_etf_holdings.T.reset_index().rename(columns={'index': 'etf_ticker'})\
        .assign(btc_holdings = lambda x: x['btc_holdings'].astype(float))\
        .assign(date=lambda x: pd.to_datetime(x['date'], format='mixed'))\
        .assign(date=lambda x: x['date'].dt.strftime('%Y-%m-%d'))\
        .to_sql('btc_etf_holdings', conn, if_exists='append', index=False)
    conn.close()
    

def get_holdings():
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    existing_holdings = pd.read_sql('select * from btc_etf_holdings', conn)
    conn.close()
    return existing_holdings.drop_duplicates().sort_values(['etf_ticker','date'])


def insert_records(ticker: str, date: str, btc_holdings: float, btc_mv: float, average_mv=np.nan, cash_holdings=np.nan):
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')

    pd.DataFrame({
        'etf_ticker': [ticker],
        'date': [date],
        'btc_holdings': [btc_holdings],
        'btc_mv': [btc_mv],
        'average_mv': [average_mv],
        'cash_holdings': [cash_holdings]
    }).to_sql('btc_etf_holdings', conn, if_exists='append', index=False)
    conn.close()


def fill_missing_price(method='mv/quant', price=np.nan, date=np.nan):
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    if method == 'mv/quant':
        conn.execute("update btc_etf_holdings set average_mv = btc_mv/btc_holdings where average_mv is null")
        conn.commit()
        conn.close()
    elif method == 'value':
        sql = f"""
        update btc_etf_holdings 
        set average_mv = {price} 
        where average_mv is null 
        and date = '{date}' 
        """
        conn.execute(sql)
        conn.commit()
        conn.close()


def fill_missing_holdings(method='mv/price'):
    import sqlite3
    from src.config import DB_DIR
    if method == 'mv/price':
        conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
        conn.execute("update btc_etf_holdings set btc_holdings = btc_mv/average_mv where btc_holdings is null")
        conn.commit()
        conn.close()


def recalc_holdings(ticker, method='mv/price'):
    import sqlite3
    from src.config import DB_DIR
    if method == 'mv/price':
        conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
        conn.execute(f"update btc_etf_holdings set btc_holdings = 1e9*btc_mv/average_mv where etf_ticker = '{ticker}' ")
        conn.commit()
        conn.close()


def correct_date(ticker, original_date, new_date):
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    sql = f"""
    update btc_etf_holdings set date = '{new_date}' 
    where etf_ticker = '{ticker}' and date = '{original_date}'
    """
    conn.execute(sql)
    conn.commit()
    conn.close()


def main():
    print("Downloading BTC ETF holdings...")
    btc_etf_holdings = scrape_btc_etf_holdings(3)
    write_to_db(btc_etf_holdings)
    #print(get_holdings().to_string())


if __name__ == '__main__':

    #import sqlite3
    #from src.config import DB_DIR
    #conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    #existing_holdings = pd.read_sql('select * from etf_holdings', conn)
    #print(existing_holdings.to_string())
    
    main()