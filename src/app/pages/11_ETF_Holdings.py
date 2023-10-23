from pathlib import Path
import os, sys
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))
import src.config as cfg
from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.utils.plotting_utils import plot_line_chart

import streamlit as st
import pandas as pd
import plotly.express as px


@st.cache_data
def get_etf_holdings_data(dt: str):
    df = pd.read_csv(ROOT_DIR/f"data/equity_market/1_ishares_etf/ishares_holdings_{dt}.csv")
    # Convert the 'Weight (%)' column to numeric, handling any errors that might arise
    df['Weight (%)'] = pd.to_numeric(df['Weight (%)'], errors='coerce')
    df.rename(columns={'as_of_date': 'Date'}, inplace=True)
    #df['Market Value'] = df['Market Value'].fillna(0.0).apply(lambda x: float(x.replace(',','')) )/1e6  # in billions
    return df


@st.cache_data
def get_etf_sector_weights(etf_holdings_df):
    # Group by 'Sector' and 'etf_ticker' and calculate the total weight
    grouped_df = etf_holdings_df.groupby(['Sector', 'etf_name'])['Weight (%)'].sum().reset_index()

    # Pivot the data for the heatmap
    pivot_df = grouped_df.pivot(columns='Sector', index='etf_name', values='Weight (%)')
    return pivot_df


def etf_sector_heatmap(pivot_df):
    # Create heatmap
    fig = px.imshow(pivot_df, zmin=0, zmax=100,
                    labels=dict(x="Sector", y="ETF Name", color="Weight (%)"),
                    x=pivot_df.columns, y=pivot_df.index,
                    text_auto=True, aspect='auto')

    fig.update_layout(
        title_text="<b>Sector Weights of iShares ETFs</b>",  # Title in bold
        title_x=0,  # Align title to center
        autosize=True,  # The graph will resize itself to the size of the container
        height=50*len(pivot_df.index), width=800  # You can adjust the height to your preference
    )

    return fig


def get_all_available_dates():
    files=os.listdir(ROOT_DIR/f"data/equity_market/1_ishares_etf")
    available_dates=list(set([f.replace('ishares_holdings_','').replace('.csv','') for f in files if '.csv' in f]))
    available_dates.sort()
    return available_dates


tab_1, tab_2, tab_3 = st.tabs(['Sector Weights', 'ETF Holdings Detail', 'Historical Flow'])


with tab_1:
    dt=st.selectbox("Date of analysis",get_all_available_dates(), key='sector_weights_dt')
    etf_holdings_df=get_etf_holdings_data(dt)
    etf_sector_weights=get_etf_sector_weights(etf_holdings_df)
    st.plotly_chart(etf_sector_heatmap(etf_sector_weights)) # Show the plot using streamlit

with tab_2:
    etf_name=st.selectbox("ETF Name:",etf_holdings_df.etf_name.sort_values().unique().tolist())
    rel_cols=st.multiselect(
        "Data Field:",
        etf_holdings_df.columns.tolist(),
        ['Date','Ticker','Name','Sector','Weight (%)','Price','Market Value'],
        key='etf_holdings_rel_cols'
    )
    st.dataframe(
        etf_holdings_df
        .query(f"etf_name == '{etf_name}' ")
        .sort_values("Weight (%)", ascending=False)
        .reindex(rel_cols,axis=1)
        .reset_index(drop=True)
    )

with tab_3:
    # TODO: add a logic to get all the available ETF names.
    dt = st.selectbox("Date of analysis", get_all_available_dates(), key='historical_flow_dt')
    etf_holdings_df = get_etf_holdings_data(dt)
    etf_name = st.selectbox("ETF Name:",
                            etf_holdings_df.etf_name.unique().tolist(),
                            key='historical_flow_etf_name')

    files = os.listdir(cfg.ETF_CACHE_DIR)
    import re
    etf_of_interest = []
    for f in files:
        # f match pattern ishares_holdings_2020-12-31.csv using regex
        pattern = r"ishares_holdings_(\d{4}-\d{2}-\d{2}).csv"
        match = re.search(pattern, f)
        if match:
            etf_df_dt = pd.read_csv(cfg.ETF_CACHE_DIR/f)
            if etf_name in etf_df_dt.etf_name.unique().tolist():
                the_etf_data=df_filter(etf_df_dt, {'etf_name':etf_name})  # filter out the ETF data of interest
                etf_of_interest.append(the_etf_data)
    etf_of_interest=pd.concat(etf_of_interest)\
        .assign(as_of_date=lambda x: pd.to_datetime(x.as_of_date))\
        .assign(market_value=lambda x: x['Market Value'].apply(lambda x: float(x.replace(',','')) )/1e6)
    total_mv = etf_of_interest.groupby(['as_of_date','etf_name'])['market_value'].sum()
    # plot the total market value of the ETF using plotly
    fig = px.line(total_mv.reset_index(), x='as_of_date', y='market_value', color='etf_name')
    st.plotly_chart(fig)