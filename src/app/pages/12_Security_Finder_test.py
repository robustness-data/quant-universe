import os, sys, logging
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
    #sys.path.append(str(ROOT_DIR/'src'))

import src.config as cfg
import src.data.utils as hp
from src.data.equity_api_yfinance import Stock

import datetime
import itertools
from tqdm import tqdm
import pandas as pd
import streamlit as st
import plotly.express as px

us_universe_df = pd.read_csv(ROOT_DIR/'data'/'equity_market'/'3_fundamental'/'raw'/'us_2023-07-10.csv')
all_us_tics = us_universe_df.Ticker.unique().tolist()

tic=st.selectbox("Select a Ticker", [None]+all_us_tics)

if tic is None:
    st.stop()

#asset = Stock.load_stock('300750.SZ')
asset = Stock.load_stock(tic)
asset.history(period='max')
asset.get_financials()

tab_names = ['Description', 'Performance', 'Financials', 'Module Inspector']
tab_desc, tab_perf, tab_fin, tab_test = st.tabs(tab_names)

with tab_desc:
    st.write(asset.info)
    yf_info_type = pd.DataFrame({'info_type': {k: type(v).__name__ for k, v in asset.info.items()}}).info_type.to_dict()
    info_df = pd.DataFrame({'meta_info':{k: asset.info.get(k) for k, v in yf_info_type.items() if v in ('str', 'float', 'int')}})

    def display_company_info(company_info):
        st.title(company_info.get("longName", 'N/A'))
        st.write(f"Sector: {company_info.get('sector', 'N/A')}")
        st.write(f"Industry: {company_info.get('industry', 'N/A')}")
        st.write(f"Address: {company_info.get('address1', 'N/A')}, {company_info.get('city', 'N/A')}, {company_info.get('state', 'N/A')}, {company_info.get('zip', 'N/A')}, {company_info.get('country', 'N/A')}")
        st.write(f"Phone: {company_info.get('phone', 'N/A')}")
        st.write(f"Website: {company_info.get('website', 'N/A')}")
        st.write(f"Summary: {company_info.get('longBusinessSummary', 'N/A')}")
        st.write(f"Full time employees: {company_info.get('fullTimeEmployees', 'N/A')}")

    display_company_info(asset.info)
        

with tab_perf:
    st.title("Performance")
    hist_price = asset._history.rename(pd.to_datetime).resample('B').ffill().reset_index()
    st.plotly_chart(hp.plot_line_chart(hist_price))

with tab_test:
    st.title("Module Inspector")

    # make a selectbox to select the attribute
    attr = st.selectbox("Select an attribute", [None]+list(dir(asset)))

    if attr is not None:
        attr_value = getattr(asset, attr)
    else:
        st.stop()

    # check if the attribute is callable
    if callable(attr_value):
        # if callable, call the attribute and display the result
        st.write(f"{attr} is a callable attribute with type {type(attr_value).__name__}, calling it now...")
        try:
            st.write(attr_value())
            if isinstance(attr_value(), pd.DataFrame):
                st.write("The size of the dataframe is: ", attr_value().shape)
        except Exception as e:
            logging.CRITICAL(f"Error in calling the attribute: {e}.")

    else:
        # if not callable, display the attribute value
        st.write(f"{attr} is not a callable attribute, displaying its value...")
        st.write(attr_value)
        if isinstance(attr_value, pd.DataFrame):
            st.write("The size of the dataframe is: ", attr_value.shape)

        # if the attr_value is again a class, display its attributes as well
        if isinstance(attr_value, object):
            st.write(f"{attr} is an object, displaying its attributes...")
            st.write(dir(attr_value))