import os
import datetime
import itertools
from tqdm import tqdm

from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent

import pandas as pd
import streamlit as st
import plotly.express as px


CACHE_DIR=ROOT_DIR/'data'/'equity_market'/'3_fundamental'
print(CACHE_DIR)


@st.cache_data
def load_stock_universe_data():
    univ_list = []
    for f in os.listdir(CACHE_DIR/'raw'):
        index, as_of_date = f.replace('.csv','').split('_')
        univ_df = pd.read_csv(CACHE_DIR/'raw'/f).assign(Universe=index).assign(Date=as_of_date)
        univ_list.append(univ_df)
        del univ_df
    all_univ_df = pd.concat(univ_list)
    all_univ_df = all_univ_df.dropna(subset='Market Capitalization')
    return all_univ_df


universe_df = load_stock_universe_data()
univ_names = universe_df.Universe.unique().tolist()
univ_names.sort()
univ_dates = universe_df.Date.unique().tolist()
univ_dates.sort()
cols=universe_df.columns.tolist()
cols.sort()
sector_names=universe_df.Sector.sort_values().unique().tolist()
sector_names.sort()
industry_names=universe_df.Industry.sort_values().unique().tolist()
industry_names.sort()
sector_industry_map = universe_df.groupby(['Sector']).apply(lambda x: x.Industry.unique().tolist()).to_dict()
#st.write(sector_industry_map)

univ_names_renamer = {
    'US All Universe': 'us',
    'NASDAQ Composite': 'nasdaq-composite',
    'NASDAQ 100': 'nasdaq-100',
    'NASDAQ Golden Dragon China': 'nasdaq-china',
    'NASDAQ Biotech': 'nasdaq-biotech',
    'S&P 500': 'sp500',
    'Russell 1000': 'r1k',
    'Russell 2000': 'r2k',
    'Russell 3000': 'r3k',
    'China All Universe': 'china',
    'Hong Kong Universe': 'hongkong'
}

category_dict = {
    "General Meta": [
            'Ticker', 'Description', 'Sector', 'Industry', 'Country',
            'Number of Employees', 'Total Shares Outstanding', 'Shares Float',
            "Price", 'Market Capitalization', 'Money Flow (14)',
            "Recent Earnings Date", "Upcoming Earnings Date", "Technical Rating"
        ],
    "Performance": [
        'Change', 'Change %', "Change from Open %", 'Change 1W, %', 'Change 1M, %',
        'Weekly Performance', "Monthly Performance", '3-Month Performance', '6-Month Performance',
        'Yearly Performance', '5Y Performance', 'YTD Performance',
        '1-Month High', '3-Month High', '6-Month High', '52 Week High', 'All Time High',
        '1-Month Low', '3-Month Low', '6-Month Low', '52 Week Low', 'All Time Low',
        "1-Year Beta"
    ],
    "Trend": [
        'Exponential Moving Average (5)',
        'Exponential Moving Average (10)',
        'Exponential Moving Average (20)',
        'Exponential Moving Average (30)',
        'Exponential Moving Average (50)',
        'Exponential Moving Average (100)',
        'Simple Moving Average (5)',
        'Simple Moving Average (10)',
        'Simple Moving Average (20)',
        'Simple Moving Average (30)',
        'Simple Moving Average (50)',
        'Simple Moving Average (100)',
        'Simple Moving Average (200)',
    ],
    "Risk": [
        'Volatility', 'Volatility Week', 'Volatility Month'
    ],
    "Volume": [
        'Volume', 'Volume*Price', 'Volume Weighted Average Price',  'Volume Weighted Moving Average (20)',
        'Average Volume (10 day)', 'Average Volume (30 day)', 'Average Volume (60 day)', 'Average Volume (90 day)',
    ],
    "Balance Sheet": [
        'Total Liabilities (FY)', 'Cash & Equivalents (FY)', 'Cash and short term investments (FY)',
        'Total Assets (MRQ)', 'Total Current Assets (MRQ)', 'Cash & Equivalents (MRQ)', 'Cash and short term investments (MRQ)',
        'Total Liabilities (MRQ)', 'Total Debt (MRQ)', 'Net Debt (MRQ)',
        'Enterprise Value (MRQ)',
        'Debt to Equity Ratio (MRQ)','Quick Ratio (MRQ)', 'Current Ratio (MRQ)',

        'Total Debt (Annual YoY Growth)',
        'Total Debt (Quarterly YoY Growth)',
        'Total Debt (Quarterly QoQ Growth)',
        'Total Assets (Annual YoY Growth)',
        'Total Assets (Quarterly YoY Growth)',
        'Total Assets (Quarterly QoQ Growth)'
    ],
    "Profitability": [
        'Gross Margin (FY)', 'Net Margin (FY)', 'Free Cash Flow Margin (FY)', 'Operating Margin (FY)',
        'Gross Margin (TTM)', 'Net Margin (TTM)', 'Free Cash Flow Margin (TTM)', 'Operating Margin (TTM)',
        'Pretax Margin (TTM)',
        'Return on Assets (TTM)', 'Return on Equity (TTM)', 'Return on Invested Capital (TTM)',
    ],
    "Income Statement": [
        'Total Revenue (FY)', 'Gross Profit (FY)', 'Net Income (FY)', 'Dividends Paid (FY)',
        'EBITDA (TTM)', 'Basic EPS (TTM)', 'EPS Diluted (TTM)',
        'Revenue per Employee (FY)', 'Dividends per Share (FY)', 'EPS Diluted (FY)', 'Basic EPS (FY)',
        'EPS Forecast (MRQ)', 'EPS Diluted (MRQ)', "Dividends per Share (MRQ)", 'Gross Profit (MRQ)',

        'Revenue (Annual YoY Growth)', 'EPS Diluted (Annual YoY Growth)', 'EBITDA (Annual YoY Growth)',
        'Free Cash Flow (Annual YoY Growth)', 'Gross Profit (Annual YoY Growth)', 'Net Income (Annual YoY Growth)',
        "Dividends per share (Annual YoY Growth)",

        'Revenue (Quarterly QoQ Growth)', 'EPS Diluted (Quarterly QoQ Growth)', 'EBITDA (Quarterly QoQ Growth)',
        'Free Cash Flow (Quarterly QoQ Growth)', 'Gross Profit (Quarterly QoQ Growth)', 'Net Income (Quarterly QoQ Growth)',

        'Revenue (Quarterly YoY Growth)', 'EPS Diluted (Quarterly YoY Growth)', 'EBITDA (Quarterly YoY Growth)',
        'Free Cash Flow (Quarterly YoY Growth)', 'Gross Profit (Quarterly YoY Growth)', 'Net Income (Quarterly YoY Growth)',

        'Revenue (TTM YoY Growth)', 'EPS Diluted (TTM YoY Growth)', 'EBITDA (TTM YoY Growth)',
        'Free Cash Flow (TTM YoY Growth)', 'Gross Profit (TTM YoY Growth)', 'Net Income (TTM YoY Growth)',
    ],
    'Investment':[
        'Research & development Ratio (TTM)', 'Research & development Ratio (FY)',
    ],
    "Valuation": [
        'Price to Book (FY)', 'Price to Book (MRQ)', 'Price to Sales (FY)', "Dividend Yield Forward",
        'Price to Earnings Ratio (TTM)', 'Enterprise Value/EBITDA (TTM)',
        'Price to Free Cash Flow (TTM)', 'Price to Revenue Ratio (TTM)',
    ],

}


def universe_filter(df, filter_dict):
    df = df.copy()
    for column, value in filter_dict.items():
        if value is not None:
            df = df[df[column] == value]
    return df


# ============================================= Page Content Starts ================================================== #



scatter_tab, security_tab = st.tabs(['Fundamental Scatter', 'Security Analytics'])


with scatter_tab:
    # select universe and date
    univ_col1, univ_col2 = st.columns(2)
    univ_name = univ_names_renamer.get(univ_col1.selectbox("Universe", [None]+list(univ_names_renamer.keys())), None)
    as_of_date = univ_col2.selectbox("As of Date", univ_dates)

    # select the industry and sector
    filter_col1, filter_col2, filter_col3 = st.columns(3)
    sector_filter = filter_col1.selectbox("Sector Filter", [None]+sector_names)
    industry_filter = filter_col2.selectbox("Industry Filter", [None]+sector_industry_map.get(sector_filter,industry_names))
    group_var = filter_col3.selectbox("Group by:", [None, 'Sector', 'Industry', 'Technical Rating', 'Universe'])

    # select metrics to plot
    available_vars = sorted(list(itertools.chain(*category_dict.values())))
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
        data_frame=universe_filter(universe_df, univ_filter_dict),
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



