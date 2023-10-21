import sys
import itertools

from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.data.utils import universe_filter
from data.equity_data.tradingview import TradingView
from data.equity_data.yfinance_old import Stock

import streamlit as st
import plotly.express as px


CACHE_DIR=ROOT_DIR/'data'/'equity_market'/'3_fundamental'
print(CACHE_DIR)


try:
    tv = TradingView()
    st.write("TradingView Fundamental Data Loaded Successfully!")
except Exception as e:
    st.write(f"Failed to load TradingView Fundamental Data: {e}")

# ---------------------------------------- #
# Trading View Fundamental Data
# ---------------------------------------- #

scatter_tab, security_tab = st.tabs(['Fundamental Scatter', 'Security Analytics'])


with scatter_tab:

    # select universe and date
    univ_col1, univ_col2 = st.columns(2)
    user_selected_univ = univ_col1.selectbox("Universe", [None]+list(tv.renamer.keys()))
    univ_name = tv.renamer.get(user_selected_univ, None)
    as_of_date = univ_col2.selectbox("As of Date", tv.dates)

    # select the industry and sector
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    sector_filter = filter_col1.selectbox("Sector Filter", [None]+tv.sector_names)
    industry_filter = filter_col2.selectbox("Industry Filter",
                                            [None]+tv.sector_industry_map.get(sector_filter, tv.industry_names))
    group_var = filter_col3.selectbox("Group by:", [None, 'Sector', 'Industry', 'Technical Rating', 'Universe'])

    # select metrics to plot
    available_vars = sorted(list(itertools.chain(*tv.category_dict.values())))
    scatter_col1, scatter_col2, group_col3 = st.columns(3)
    metrics_x = scatter_col1.selectbox("Metrics x:", available_vars)
    metrics_y = scatter_col2.selectbox("Metrics y:", available_vars)


    #st.write([x for x in cols if x not in available_vars])

    univ_filter_dict = {
        'Universe': univ_name,
        'Date': as_of_date,
        'Sector': sector_filter,
        'Industry': industry_filter
    }


    fig=px.scatter(
        data_frame=universe_filter(tv.data_df, univ_filter_dict),
        x=metrics_x,
        y=metrics_y,
        color=group_var,
        hover_name='Description',
        size='Market Capitalization',
    )

    fig.update_layout(
        #title_text="<b>Sector Weights of iShares ETFs</b>",  # Title in bold
        #title_x=0,  # Align title to center
        autosize=True,  # The graph will resize itself to the size of the container
        height=600, width=900  # You can adjust the height to your preference
    )

    st.plotly_chart(fig)

with security_tab:

    asset = Stock('AAPL')
    st.write(asset.meta_info)

