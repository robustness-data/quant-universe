import sys
import io
from pathlib import Path
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.data.equity_data.tradingview import TradingView
from src.data.equity_data.yfinance import Stock
from src.config import DB_DIR
from src.data.equity_data.etf.holdings import get_etf_holdings_text, scrape_webpage, spdr_etfs_urls
from src.utils.streamlit_utils import filter_dataframe

import streamlit as st
import plotly.express as px
import sqlite3
import datetime
import pandas as pd
import numpy as np

# change the page icon to a coffee
st.set_page_config(page_title='ETF Analysis', page_icon=':coffee:')


def load_etf_holdings():

    conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
    query = """
    select *
    from etf_holdings
    """
    df=pd.read_sql(query,conn)
    conn.close()
    return df

def get_xbi_holdings():
    spdr_xbi = get_etf_holdings_text(spdr_etfs_urls['XBI'])
    xbi_holdings = pd.read_excel(spdr_xbi, skiprows=4).dropna()
    return xbi_holdings

# ---------------------------- Session State ---------------------------- #
st.session_state['etf_holdings'] = None
st.session_state['xbi_holdings'] = None

if st.button("读取ETF持仓数据"):
    st.session_state['etf_holdings'] = load_etf_holdings()

if st.button("读取XBI持仓数据"):
    st.session_state['xbi_holdings'] = get_xbi_holdings()

if st.session_state['etf_holdings'] is not None:
    st.write(st.session_state['etf_holdings'].head())

with st.expander("GS Healthcare Screener"):
    
    gs_hc_path = st.file_uploader("Upload screener file", type="csv")
    gs_data = pd.read_csv(gs_hc_path, skiprows=1)
    filtered_gs_data = filter_dataframe(gs_data)
    filtered_gs_data = filtered_gs_data\
        .assign(target_return=
            lambda x: x['Upside/Downside (%)']\
            .apply(lambda y: y.replace('(','-').replace(')','').replace('%','').replace('--','nan'))\
            .astype(float)
        )
    st.write(filtered_gs_data, use_container_width=True)

with st.expander("XBI/LABU Analysis"):
    if st.session_state['xbi_holdings'] is not None:
        filtered_xbi = filter_dataframe(st.session_state['xbi_holdings'])
        st.write(filtered_xbi, use_container_width=True)
