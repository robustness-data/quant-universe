import pandas as pd
import streamlit as st
import plotly.express as px

from pathlib import Path
import os
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent


@st.cache_data
def prepare_shibor_quotes():
    df = pd.read_excel(ROOT_DIR/'data/macro/CFETS/Shibor_Quotes_Historical_Data.xlsx')  # Load data
    df = df[:-2]  # Drop the last two rows
    df['Date'] = pd.to_datetime(df['Date'])  # Convert the 'Date' column to datetime
    # Convert columns for different maturities to numeric
    df=df.astype({c: float for c in ["O/N", "1W", "2W", "1M", "3M", "6M", "9M", "1Y"]})
    df.columns.name='Tenor'

    quotes_long=df.set_index(['Contributor','Date']).stack().rename('ShiborQuote').reset_index()
    quotes_diff_wide=(
        quotes_long.set_index(['Date','Contributor','Tenor']).ShiborQuote
        .unstack().unstack().sort_index(ascending=True).diff()
    )
    quotes_diff_long=quotes_diff_wide.stack().stack().rename('ShiborQuote').reset_index()
    corr_banks = quotes_diff_wide.ewm(halflife=30).corr()

    return quotes_long, quotes_diff_long, quotes_diff_wide, corr_banks


def get_corr_banks(corr_banks, dt:str, tenor: str):
    corr_df=corr_banks.xs(dt).loc[tenor, tenor]
    corr_df.index.name='Bank-x'
    corr_df.columns.name='Bank-y'
    corr_df=corr_df.applymap(lambda x: int(x*100))
    return corr_df


def plotly_heatmap(pivot_df, val_col, x_label=None, y_label=None, title=None):
    fig = px.imshow(pivot_df,
                    zmin=-100, zmax=100, color_continuous_scale='rdbu',
                    labels=dict(x=x_label, y=y_label, color=val_col),
                    x=pivot_df.columns, y=pivot_df.index,
                    text_auto=True, aspect='auto')

    if title is None:
        title=''
    fig.update_layout(
        title_text=f"<b>{title}</b>",  title_x=0,
        autosize=True,  # The graph will resize itself to the size of the container
        height=500,width=600  # You can adjust the height to your preference
    )

    return fig


quotes_long, quotes_diff_long, quotes_diff_wide, corr_banks = prepare_shibor_quotes()
tenor='3M'
dt='2023-6-30'
st.write(
    plotly_heatmap(
        get_corr_banks(corr_banks,dt,tenor),
        val_col='Correlation (%)',
        title=f"Correlation of Shibor Quote Changes for Tenor {tenor} on {dt}")
)