import os, sys, logging
from pathlib import Path
ROOT_DIR = Path(__file__).parent.parent.parent
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

#asset = Stock.load_stock('300750.SZ')
asset = Stock.load_stock('TEL')
asset.history(period='max')
asset.get_financials()



#st.write(get_method_names(asset))
#tab_description = st.expander('Description')
#with tab_description:
#    st.write(asset.info)

tab_names = ['Description', 'Performance', 'Financials', 'Earnings', 'Dividends', 'Module Inspector']
tab_desc, tab_perf, tab_fin, tab_earn, tab_div, tab_test = st.tabs(tab_names)
with tab_desc:
    st.write(asset.info)
    yf_info_type = pd.DataFrame({'info_type': {k: type(v).__name__ for k, v in asset.info.items()}}).info_type.to_dict()
    info_df = pd.DataFrame({'meta_info':{k: asset.info.get(k) for k, v in yf_info_type.items() if v in ('str', 'float', 'int')}})
    st.dataframe(info_df)


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
    #st.write(asset._history)
    hist_price = asset._history.rename(pd.to_datetime).resample('B').ffill().reset_index()
    st.write(hist_price.columns.tolist())

    import plotly.graph_objects as go
    from plotly.subplots import make_subplots
    # Create subplot with 2 rows
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                vertical_spacing=0.1, subplot_titles=('Stock Price', 'Volume'), 
                row_heights=[0.8, 0.2])

    df = hist_price
    # Add candlestick track on 1st plot
    fig.add_trace(
        go.Candlestick(x=df['Date'], open=df['Open'], high=df['High'], 
                       low=df['Low'], close=df['Close'], name='Price'),
        row=1, col=1
    )

    # Add volume bar chart on 2nd plot
    fig.add_trace(go.Bar(x=df['Date'], y=df['Volume'], showlegend=False), row=2, col=1)

    # Add range slider
    fig.update_layout(xaxis=dict(
        rangeslider=dict(visible=True),
        type='date'
    ))

    st.plotly_chart(fig)

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