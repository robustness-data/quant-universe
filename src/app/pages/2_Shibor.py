import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

from pathlib import Path
import os
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent

tab_corr, tab_ts = st.tabs(['Quote Correlation','Quote Time-series'])

@st.cache_data
def prepare_shibor_quotes():
    df = pd.read_excel(ROOT_DIR/'data/macro/CFETS/Shibor_Quotes_Historical_Data.xlsx')  # Load data
    df = df[:-2]  # Drop the last two rows
    df['Date'] = pd.to_datetime(df['Date'])  # Convert the 'Date' column to datetime
    # Convert columns for different maturities to numeric
    df=df.astype({c: float for c in ["O/N", "1W", "2W", "1M", "3M", "6M", "9M", "1Y"]})
    df.columns.name='Tenor'

    quotes_long=df.set_index(['Contributor','Date']).stack().rename('ShiborQuote').reset_index()
    quotes_diff_wide_d=(
        quotes_long.set_index(['Date','Contributor','Tenor']).ShiborQuote
        .unstack().unstack().sort_index(ascending=True).diff()
    )
    quotes_diff_wide_w=(
        quotes_long.set_index(['Date', 'Contributor', 'Tenor']).ShiborQuote
        .unstack().unstack().sort_index(ascending=True).resample('W-FRI').last().diff()
    )
    quotes_diff_long=quotes_diff_wide_d.stack().stack().rename('ShiborQuote').reset_index()

    return quotes_long, quotes_diff_long, quotes_diff_wide_d, quotes_diff_wide_w


@st.cache_resource
def get_corr_banks(quotes_diff_wide, dt:str, tenor: str, ewm_hl:int):
    corr_banks = quotes_diff_wide.ewm(halflife=ewm_hl).corr()
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


quotes_long, quotes_diff_long, quotes_diff_wide_d, quotes_diff_wide_w = prepare_shibor_quotes()

with tab_corr:
    col1, col2, col3, col4 = st.columns(4)

    # correlation plot of quotes change
    tenor = col1.selectbox('Tenor', ("O/N", "1W", "2W", "1M", "3M", "6M", "9M", "1Y"), key='corr', index=3)
    freq = col2.selectbox("Return frequency:",('Weekly','Daily'))
    shibor_ret_data_map={'Daily': quotes_diff_wide_d, 'Weekly': quotes_diff_wide_w}
    shibor_rel_dates=tuple([x.date().isoformat()
                            for x in shibor_ret_data_map.get(freq).sort_index(ascending=False).index[1:].unique().tolist()])
    dt = col3.selectbox('Date', index=0, options=shibor_rel_dates)
    #ewm_hl = col4.slider("EWM Halflife in Days:", min_value=30, max_value=180, step=5, value=30)
    ewm_hl = col4.selectbox('EWM Half-life', (30,60,120,180))

    st.write(
        plotly_heatmap(
            get_corr_banks(shibor_ret_data_map.get(freq),dt,tenor,ewm_hl),
            val_col='Correlation (%)',
            title=f"Correlation of Shibor Quote Changes for Tenor {tenor} on {dt}")
    )


# Plot time series of SHIBOR quotes
def plot_shibor_quotes_ts(quotes_range, tenor):

    quotes_range_tau = quotes_range.set_index('Tenor').xs(tenor)

    # Create a plotly figure
    fig = go.Figure()

    # Add a trace for the median line
    fig.add_trace(go.Scatter(
        x=quotes_range_tau['Date'],
        y=quotes_range_tau['median'],
        mode='lines',
        name='Median',
        line=dict(color='black')))

    # Add a trace for the shaded area representing the range
    fig.add_trace(
        go.Scatter(
            x=pd.concat([quotes_range_tau['Date'],quotes_range_tau['Date'][::-1]]),
            y=pd.concat([quotes_range_tau['max'],quotes_range_tau['min'][::-1]]),
            fill='toself',
            fillcolor='rgba(128,128,128,0.3)',
            line=dict(color='rgba(255,255,255,0)'),
            hoverinfo="skip",
            name='Range'
        )
    )

    # Set plot title and labels
    fig.update_layout(
        title_text=f'{tenor} Tenor Shibor Quotes Over Time',
        xaxis_title='Date',
        yaxis_title='Quote (%)',
        autosize=False,
        width=800,
        height=500,
        showlegend=True)

    return fig


with tab_ts:
    # Group the data by date and calculate the minimum, maximum, and median
    quotes_range = quotes_long.groupby(['Tenor','Date']).ShiborQuote.agg(['min', 'max', 'median']).reset_index()
    #st.write(quotes_range)

    #col_option, col_graph = st.columns([0.2,0.8])
    tenor_shibor_ts=st.selectbox("Tenor", ("O/N", "1W", "2W", "1M", "3M", "6M", "9M", "1Y"),
                                         index=3, key='ts')
    st.plotly_chart(plot_shibor_quotes_ts(quotes_range, tenor_shibor_ts))
