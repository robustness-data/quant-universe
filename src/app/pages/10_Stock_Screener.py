import sys
import itertools
from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from utils.pandas_utils import df_filter, set_cols_numeric
from src.data.equity_data.tradingview import TradingView
from src.data.equity_data.yfinance_old import Stock

import streamlit as st
import plotly.express as px


@st.cache_resource
def load_trading_view_data():
    try:
        tv = TradingView()
        return tv
    except:
        return None

# ---------------------------------------- #
# Trading View Fundamental Data
# ---------------------------------------- #


tv = load_trading_view_data()
if tv is not None:
    st.write("TradingView Fundamental Data Loaded Successfully!")
    scatter_tab, security_tab = st.tabs(['Fundamental Scatter', 'Security Analytics'])
else:
    st.write("TradingView Fundamental Data Failed to Load!")


with scatter_tab:
    available_vars = sorted(list(itertools.chain(*tv.category_dict.values())))

    # select universe and date
    univ_col1, univ_col2 = st.columns(2)
    as_of_date = univ_col1.selectbox("As of Date", tv.dates)
    chosen_univ = tv.load_data_dt(as_of_date)
    chosen_univ_data = chosen_univ['data_dt']
    chosen_univ_label = univ_col2.selectbox("Universe", [None] + chosen_univ.get('available_univ_labels', []))
    univ_key = tv.univ_label_to_keys.get(chosen_univ_label, None)

    # select the industry, sector, and group column
    filter_col1, filter_col2, filter_col3, filter_col4 = st.columns(4)
    sector_filter = filter_col1.selectbox("Sector Filter", [None]+tv.sector_names)
    industry_filter = filter_col2.selectbox("Industry Filter",
                                            [None]+tv.sector_industry_map.get(sector_filter, tv.industry_names))
    group_var = filter_col3.selectbox("Group by:", [None, 'Sector', 'Industry', 'Technical Rating', 'Universe'])
    size_var = filter_col4.selectbox("Size by:", [None]+available_vars)

    # select metrics to plot
    scatter_col1, scatter_col2, group_col3 = st.columns(3)
    metrics_x = scatter_col1.selectbox("Metrics x:", available_vars)
    metrics_y = scatter_col2.selectbox("Metrics y:", available_vars)
    metrics_vars = list(set(list([metrics_x, metrics_y])))
    chosen_univ_data = set_cols_numeric(chosen_univ_data, metrics_vars+["Market Capitalization"])


    univ_filter_dict = {
        'Universe': univ_key,
        'Date': as_of_date,
        'Sector': sector_filter,
        'Industry': industry_filter
    }


    fig=px.scatter(
        data_frame=df_filter(chosen_univ_data, univ_filter_dict),
        x=metrics_x,
        y=metrics_y,
        color=group_var,
        hover_name='Description',
        size=size_var,
    )

    fig.update_layout(
        autosize=True,  # The graph will resize itself to the size of the container
        height=600, width=900  # You can adjust the height to your preference
    )
    st.plotly_chart(fig)

with security_tab:

    asset = Stock('AAPL')
    st.write(asset.meta_info)

