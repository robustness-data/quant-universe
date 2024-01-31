import sys, os
import itertools
from pathlib import Path
ROOT_DIR=Path(__file__).parent.parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.data.equity_data.tradingview import TradingView
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
    "FBTC": "https://www.actionsxchangerepository.fidelity.com/ShowDocument/documentExcel.htm?_fax=-18%2342%23-61%23-110%23114%2378%23117%2320%23-1%2396%2339%23-62%23-21%2386%23-100%2337%2316%2335%23-68%2391%23-66%2354%23103%23-16%2369%23-30%2358%23-20%2376%23-84%23-11%23-87%23-12%23-5%23-88%23-81%23-82%2362%2321%23-75%23-38%23-43%23-39%23-42%23-96%23-88%2388%23-45%23105%23-76%2367%23125%23123%23-122%23-5%2319%23-74%235%23-89%23-105%23-67%23126%2377%23-126%23-63%2334%2346%2383%23-20%2394%23-19%2363%2384%2373%23-122%23-128%23-66%2387%23122%2399%23-82%2357%23-31%23-81%2368%23114%2348%23-42%23-43%23112%23-89%2342%2313%2319%2329%23-117%2321%23-37%23-56%23-125%23-115%23-55%23100%23100%2383%23-5%23-100%23-91%23-14%2389%2396%23-104%23-10%2319%2372%2368%23-104%23112%23-124%23-45%2310%2382%23-111%2329%2342%2325%23-41%233%23-83%23-82%2367%23-118%23-119%233%23-98%23-11%2371%23107%23-82%23-3%23-86%23-27%23-57%23-125%2342%23119%2357%23-111%2321%2363%23-99%2317%23-25%23-81%23-24%23-58%2351%2344%23115%23-107%23-77%23114%2363%2359%23-31%2334%23112%23114%23-57%23125%2386%23-39%23-86%23-67%23-11%23-125%23",
    "EZBC": "https://www.franklintempleton.com/investments/options/exchange-traded-funds/products/39639/SINGLCLASS/franklin-bitcoin-etf/EZBC",
    'BTCO': "https://www.invesco.com/us/financial-products/etfs/product-detail?audienceType=Advisor&ticker=BTCO"
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


def scrape_gbtc_holdings():
    soup = scrape_webpage("https://etfs.grayscale.com/gbtc")
    def parse_gbtc_str(x):
        step1=x.replace('Key Fund Information','').replace('As of ','')
        as_of_date, step2 = step1.split('Assets Under Management (Non-GAAP)$')
        aum, step3 = step2.split('Base Currency')
        currency, step4 = step3.split('Shares Outstanding')
        shares_outstanding, step5 = step4.split('Sponsor')
        sponsor, step6 = step5.split('Total Expense Ratio*')
        expense_ratio, step7 = step6.split('Index Provider')
        index_provider, step8 = step7.split('Total Bitcoin in Trust')
        total_btc, step9 = step8.split('Fund Administrator')
        fund_admin, step10 = step9.split('Bitcoin per Share')
        btc_per_share, step11 = step10.split('Digital Asset Custodian')
        digital_asset_custodian, step12 = step11.split('Marketing Agent')
        marketing_agent = step12.strip()

        return {
            'as_of_date': pd.to_datetime(as_of_date).date().isoformat(),
            'aum': float(aum.replace(',','')),
            'currency': currency,
            'shares_outstanding': int(shares_outstanding.replace(',','')),
            'sponsor': sponsor,
            'expense_ratio': expense_ratio,
            'index_provider': index_provider,
            'total_btc': float(total_btc.replace(',','')),
            'fund_admin': fund_admin,
            'btc_per_share': float(btc_per_share.replace(',','')),
            'digital_asset_custodian': digital_asset_custodian,
            'marketing_agent': marketing_agent
        }
    
    for s in soup.find_all('div'):
        c = s.get('class')
        if c is None:
            continue
        if s.text.startswith("Overview"):
            continue
        if 'Tables_Tables__container_table__Tw2_H' in c:
            parsed_holdings = parse_gbtc_str(s.text)

    gbtc_info = {
        'btc_holdings': parsed_holdings['total_btc'],
        'btc_mv': parsed_holdings['aum']/1e9,
        'date': parsed_holdings['as_of_date'],
        'cash_holdings': 0.0,
    }
    gbtc_info['average_mv'] = 1e9*gbtc_info['btc_mv']/gbtc_info['btc_holdings']
    return pd.DataFrame({'GBTC': gbtc_info})


def scrape_btc_etf_holdings(max_retry: int):
    scraper_map = {
        'ARKB': scrape_arkb_holdings,
        'IBIT': scrape_ibit_holdings,
        'BRRR': scrape_brrr_holdings,
        'GBTC': scrape_gbtc_holdings,
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


def scrape_fbtc_holdings():
    import requests
    response = requests.get(BTC_ETF_Scraper.holdings_urls['FBTC'])
    if response.status_code == 200:
        fbtc_raw = response.content
    else:
        print("Failed to get response from FBTC's website.")
    # turn btype into ByteIO object
    from io import BytesIO
    holdings_tab = pd.read_excel(BytesIO(fbtc_raw), header=None)
    for i, row in holdings_tab.iterrows():
        print(i)
        row_text = ' '.join([str(x) for x in row.to_dict().values()])
        row_text = row_text.replace('nan', '').strip()
        print(row_text)
        #if row['Security Name'] == 'BITCOIN':
        #    qty = row['Quantity Held']
        #    mv = row['Market Value']
        #    print(qty, mv)


def write_to_db(btc_etf_holdings: pd.DataFrame, table_name='btc_etf_holdings'):
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    btc_etf_holdings.T.reset_index().rename(columns={'index': 'etf_ticker'})\
        .assign(btc_holdings = lambda x: x['btc_holdings'].astype(float))\
        .assign(date=lambda x: pd.to_datetime(x['date'], format='mixed'))\
        .assign(date=lambda x: x['date'].dt.strftime('%Y-%m-%d'))\
        .assign(updated_datetime=lambda x: datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))\
        .to_sql(table_name, conn, if_exists='append', index=False)
    conn.close()
    

def get_holdings():
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    existing_holdings = pd.read_sql("SELECT * FROM btc_etf_holdings", conn)\
        .drop_duplicates(keep='last')\
        .groupby(['etf_ticker','date']).apply(lambda df: df.sort_values('updated_datetime').iloc[-1])\
        .reset_index(drop=True)
    conn.close()
    return existing_holdings


def insert_records(ticker: str, date: str, btc_holdings: float, btc_mv: float, average_mv=np.nan, cash_holdings=np.nan):
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    import datetime

    pd.DataFrame({
        'etf_ticker': [ticker],
        'date': [date],
        'btc_holdings': [btc_holdings],
        'btc_mv': [btc_mv],
        'average_mv': [average_mv],
        'cash_holdings': [cash_holdings],
        'updated_datetime': [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')] 
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


def clean_up(ticker, date, new_holdings):
    fill_missing_price(method='mv/quant')
    avg_mv = get_holdings().groupby('date').average_mv.mean().to_dict()
    for date, p in avg_mv.items():
        fill_missing_price(method='value', date=date, price=p)    
    fill_missing_holdings()


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


def delete_record(ticker, date, holdings):
    import sqlite3
    from src.config import DB_DIR
    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    sql = f"""
    delete from btc_etf_holdings 
    where date = '{date}' 
    and etf_ticker = '{ticker}' 
    and btc_holdings = {holdings}
    """
    conn.execute(sql)
    conn.commit()
    conn.close()


def main():
    print("Downloading BTC ETF holdings...")
    btc_etf_holdings = scrape_btc_etf_holdings(3)
    write_to_db(btc_etf_holdings)
    #print(get_holdings().to_string())


def plot_btc_etf_holdings(dates_to_remove: list):
    fig = px.bar(
        get_holdings().set_index(['date','etf_ticker'])\
            .btc_holdings.unstack().rename(pd.to_datetime)[['ARKB','IBIT','FBTC','BTCO','BRRR','EZBC']]\
            .dropna(how='all')\
            .drop(dates_to_remove, errors='ignore')\
            .rename(columns={'ARKB': 'ARK 21Shares Bitcoin ETF', 
                            'IBIT': '贝莱德比特币ETF',
                            'FBTC': 'Fidelity比特币ETF',
                            'BRRR': 'Valkyrie Bitcoin ETF',
                            'BTCO': 'Invesco Bitcoin ETF',
                            'EZBC': '富兰克林比特币ETF'}),
        barmode='stack',
        color_discrete_sequence=['#FFD300','#FF0800','#00A86B','orange','#0080FF','#AF69EF'],
        template='plotly_dark'    
    )
    fig.update_layout(
        height=700, width=1000,
        title=f'比特币ETF持仓量',
        xaxis_title='持仓日期',
        yaxis_title='比特币持有量',
        legend_title_text=None,
        legend=dict(orientation="v",yanchor="top",y=1,xanchor="left",x=1),
        annotations=[
            dict(xref="paper",yref="paper",x=0,y=-1,showarrow=False,
                text="数据来源：ARK Invest, BlackRock, Valkyrie, Invesco, Franklin Templeton",
                font=dict(size=12,color="#ffffff"
                )
            )
        ],
    )
    fig.update_xaxes(rangebreaks=[dict(bounds=["sat", "mon"])])
    return fig


def plot_btc_etf_growth():
    fig = px.box(
        get_holdings().set_index(['date','etf_ticker'])\
            .btc_holdings.unstack().rename(pd.to_datetime)[['ARKB','IBIT','FBTC','BTCO','BRRR','EZBC','GBTC']]\
            .dropna(how='all').diff(),
        # change color
        color_discrete_sequence=['#FFD300','#FF0800','#00A86B','orange','#0080FF','#AF69EF']
    )
    fig.update_layout(
        height=700, width=1000,
        title=f'比特币ETF持仓每日增长量分布',
        xaxis_title=None,
        yaxis_title='比特币持有量变化分布',
        legend_title_text=None,
        legend=dict(orientation="v",yanchor="top",y=1,xanchor="left",x=1),
        annotations=[
            dict(xref="paper",yref="paper",x=0,y=-.1,showarrow=False,
                text="数据来源：ARK Invest, BlackRock, Valkyrie, Invesco, Franklin Templeton, Fidelity, Grayscale",
                font=dict(size=12,color="white"
                )
            )
        ],
        template='plotly_dark'
    )
    return fig


if __name__ == '__main__':

    #import sqlite3
    #from src.config import DB_DIR
    #conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    #existing_holdings = pd.read_sql('select * from etf_holdings', conn)
    #print(existing_holdings.to_string())
    
    main()