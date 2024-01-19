import sys, os
from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.utils.streamlit_utils import filter_dataframe
from src.data.equity_data.tradingview import TradingView, BigA
from src.config import DB_DIR, TV_CACHE_DIR
A_SHARE_CACHE_DIR = TV_CACHE_DIR/'raw'/'china'

import streamlit as st
import pandas as pd
import plotly.express as px

# ========================== Cache ========================== #

def update_cache():
    data = []
    for f in os.listdir(A_SHARE_CACHE_DIR):
        if f.startswith('china_') and f.endswith('.csv'):
            df = pd.read_csv(A_SHARE_CACHE_DIR/f)
            data.append(df)
    data = pd.concat(data)
    data.to_parquet(A_SHARE_CACHE_DIR/'china.parquet')

@st.cache_data
def load_cache():
    return pd.read_parquet(A_SHARE_CACHE_DIR/'china.parquet')

@st.cache_data
def load_single_day_data(analysis_date):
    stocks, etfs, foreign_etf = biga.load_data_cache_from_csv(dt=analysis_date.isoformat())
    return stocks

st.session_state['full_hist_data'] = None
st.session_state['stocks'] = None

biga = BigA()

# load data for a particular date
analysis_date = st.date_input('分析日期', value=None)
if analysis_date is not None:
    st.session_state['stocks'] = load_single_day_data(analysis_date=analysis_date)


# ========================== Main Workflow ========================== #
st.title('大A历险记')


st.button('更新缓存', on_click=update_cache)
if st.button("读取缓存"):
    st.session_state['full_hist_data'] = load_cache()
    
if st.session_state['full_hist_data'] is not None:
    st.write(st.session_state['full_hist_data'].head())

if st.session_state['stocks'] is not None:
    fig = px.imshow(
        st.session_state['stocks'].groupby(['Sector','sector_csi_level_2'])['tic']\
            .count().unstack().fillna(0).astype(int),
        zmin=0, zmax=100, color_continuous_scale='Blues', text_auto=True,
        #title='各个市值规模和行业的股票数量分布', 
        labels=dict(x='中证二级行业', y='TV行业', color='股票数量'),
        width=1200, height=800
    )
    st.plotly_chart(fig)


if st.session_state['stocks'] is not None:
    filtered_df = filter_dataframe(st.session_state['stocks'])
    st.write(filtered_df)