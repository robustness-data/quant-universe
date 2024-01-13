import sys
import itertools
from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.data.equity_data.tradingview import TradingView
from src.data.equity_data.yfinance_old import Stock
from src.config import DB_DIR
from src.data.equity_data.etf.holdings import get_etf_holdings_text, scrape_webpage, spdr_etfs_urls

import streamlit as st
import plotly.express as px
import sqlite3
import datetime
import pandas as pd


def load_etf_holdings():

    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    query = """
    select *
    from etf_holdings
    """
    df=pd.read_sql(query,conn)
    conn.close()
    return df

#spdr_xbi = get_etf_holdings_text(spdr_etfs_urls['XBI'])
#xbi_holdings = pd.read_excel(spdr_xbi, skiprows=4).dropna()

st.session_state['etf_holdings'] = None

if st.button("读取ETF持仓数据"):
    st.session_state['etf_holdings'] = load_etf_holdings()

if st.session_state['etf_holdings'] is not None:
    st.write(st.session_state['etf_holdings'].head())