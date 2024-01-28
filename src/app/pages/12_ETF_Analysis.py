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
st.set_page_config(page_title='ETF Analysis', page_icon=':coffee:', layout='wide', initial_sidebar_state='collapsed')


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

if 'etf_holdings' not in st.session_state:
    st.session_state['etf_holdings'] = load_etf_holdings()
if 'tradeview' not in st.session_state:
    st.session_state['tradeview'] = TradingView()
if 'filtered_tv_df' not in st.session_state:
    st.session_state['filtered_tv_df'] = pd.DataFrame()

dt = st.date_input('Date', datetime.date.today())

st.write(st.session_state['tradeview'].data_df.columns.tolist())

# ---------------------------------- Tabs ------------------------------------ #
tab1, tab2, tab3 = st.tabs(["TradingView Data", "GS Healthcare Screener", "XBI/LABU Analysis"])

with tab1:

    filtered_tv_df = filter_dataframe(
        df=st.session_state['tradeview'].data_df.query(f"Date == '{dt}'"), 
        default_cols=['Date','Ticker','Description','Price','Upcoming Earnings Date','Recent Earnings Date']
    )
    st.write(filtered_tv_df, use_container_width=True)
    st.session_state['filtered_tv_df'] = filtered_tv_df


with tab2:
    
    gs_hc_path = st.file_uploader("Upload screener file", type="csv")
    if gs_hc_path is not None:
        if 'gs_hc_data' not in st.session_state:
            st.session_state['gs_hc_data'] = pd.read_csv(gs_hc_path, skiprows=1)
        if 'filtered_gs_data' not in st.session_state:
            st.session_state['filtered_gs_data'] = pd.DataFrame()

        gs_data = st.session_state['gs_hc_data']
        filtered_gs_data = filter_dataframe(gs_data)
        filtered_gs_data = filtered_gs_data\
            .assign(target_return=
                lambda x: x['Upside/Downside (%)']\
                .apply(lambda y: 
                       y.replace('(','-').replace(')','').replace('%','').replace(',','').replace('--','nan'))\
                .astype(float)
            )
        st.write(filtered_gs_data, use_container_width=True)
        st.session_state['filtered_gs_data'] = filtered_gs_data


with tab3:

    if 'xbi_holdings' not in st.session_state:
        st.session_state['xbi_holdings'] = get_xbi_holdings()
    if 'xbi_meet_gs' not in st.session_state:
        st.session_state['xbi_meet_gs'] = pd.DataFrame()
    
    with st.expander('XBI Holdings'):
        xbi_holdings = st.session_state['xbi_holdings']
        filtered_xbi = filter_dataframe(xbi_holdings)
        st.write(filtered_xbi, use_container_width=True)

    with st.expander("XBI + GS"):
        if 'filtered_gs_data' in st.session_state:
            filtered_gs_data = st.session_state['filtered_gs_data']
            xbi_meet_gs = filtered_xbi.merge(filtered_gs_data, on='Ticker', how='left')
            st.session_state['xbi_meet_gs'] = xbi_meet_gs
            st.write(xbi_meet_gs)

        if st.button("Plot rating distribution"):
            # fill missing values of column Rating with 'Not Covered'
            plot_data = xbi_meet_gs.copy()
            plot_data.loc[plot_data['Rating'].isna(), 'Rating'] = 'Not Covered'
            rating_weight = plot_data.groupby('Rating').Weight.sum().rename("Weight (%)").reset_index()
            fig = px.pie(rating_weight, values='Weight (%)', names='Rating', title='GS Rating Distribution of XBI Holdings')
            # make legend on the pie
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)
        
        if st.button("Plot Target Price Return"):
            # fill missing values of column Rating with 'Not Covered'
            plot_data = xbi_meet_gs.copy()
            # create groups of returns based on target_return
            plot_data['target_return_group'] = pd.cut(plot_data['target_return'], 
                                                      bins=[-100, -50, -25, 0, 25, 50, 100], 
                                                      labels=['[-inf,-50%]', '[-50%,-25%]', '[-25%,0%]', '[0%,25%]', '[25%,50%]', '[50%,inf]'])
            rating_weight = plot_data.groupby('target_return_group').Weight.sum().rename("Weight (%)").reset_index()
            fig = px.pie(rating_weight, values='Weight (%)', names='target_return_group', 
                         title='GS Target Price Return Distribution of XBI Holdings',
                         category_orders={'target_return_group':
                                          ['[-inf,-50%]', '[-50%,-25%]', '[-25%,0%]', '[0%,25%]', '[25%,50%]', '[50%,inf]']})
            # make legend on the pie
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig)

    with st.expander("XBI + TV"):
        xbi_holdings = st.session_state['xbi_holdings']
        tv_data = st.session_state['filtered_tv_df']
        st.write(st.session_state['filtered_tv_df'].columns.tolist())
        xbi_meet_tv = xbi_holdings.merge(tv_data, on='Ticker', how='left')
        st.write(xbi_meet_tv)
