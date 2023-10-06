import os, sys
import datetime
import itertools
from tqdm import tqdm

from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

import src.data.utils as hp
from src.data.tradingview import TradingViewFundamental
#from src.data.yfinance import Stock, StockUniverse
from src.data.equity_api_yfinance import Stock

import pandas as pd
import streamlit as st
import plotly.express as px


CACHE_DIR=ROOT_DIR/'data'/'equity_market'/'3_fundamental'
print(CACHE_DIR)

def get_method_names(obj):
    return [attr for attr in dir(obj) if callable(getattr(obj, attr))]


@st.cache_resource
def load_stock(ticker):
    return Stock(ticker)

asset = load_stock('NVDA')

#st.write(get_method_names(asset))

#tab_description = st.expander('Description')
#with tab_description:
#    st.write(asset.info)

tab_names = ['Description', 'Performance', 'Financials', 'Earnings', 'Dividends', 'Sustainability']
tab_desc, tab_perf, tab_fin, tab_earn, tab_div, tab_sust = st.tabs(tab_names)
with tab_desc:
    st.write(asset.info)
    st.write(asset.earnings_dates)

with tab_perf:
    # plot the time series of stock price using plotly express line chart
    fig = px.line(asset.history(period='max'), y='Close')
    fig.update_layout(title='Stock Price', xaxis_title='Date', yaxis_title='Price')
    st.plotly_chart(fig, use_container_width=True)

    cand = hp.plot_candlestick_chart(asset.history(period='1Y', interval='1H'))
    st.plotly_chart(cand, use_container_width=True)



with tab_fin:
    financials_df = asset.financials.copy()
    financials_df.columns.name = 'Fiscal Year'
    financials_df.index.name = 'Item'
    financials_df = financials_df.stack().rename('value').reset_index()
    #st.write(financials_df)
    items_to_plot=st.multiselect('Select columns', asset.financials.index.tolist())
    
    # given the long format table, plot the bar chart using plotly express for the selected items
    fig = px.bar(financials_df[financials_df['Item'].isin(items_to_plot)], x='Fiscal Year', y='value', color='Item')
    fig.update_layout(title='Financials', xaxis_title='Fiscal Year', yaxis_title='Value')
    st.plotly_chart(fig, use_container_width=True)

with tab_earn:
    # replicate the same process for earnings
    st.write(asset.earnings_dates)




# write a function to process the fundamental data and return a long format table
def process_fundamental_data(asset, fundamental_data):
    df = fundamental_data.copy()
    df.columns.name = 'Fiscal Year'
    df.index.name = 'Item'
    df = df.stack().rename('value').reset_index()
    df['Ticker'] = asset.ticker
    return df