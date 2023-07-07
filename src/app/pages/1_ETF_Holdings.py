import streamlit as st
import pandas as pd
import plotly.express as px


from pathlib import Path
import os
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent


@st.cache_data
def get_etf_holdings_data(dt: str):
    df = pd.read_csv(ROOT_DIR/f"data/equity_market/1_etf/ishares_holdings_{dt}.csv")
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
    files=os.listdir(ROOT_DIR/f"data/equity_market/1_etf")
    available_dates=list(set([f.replace('ishares_holdings_','').replace('.csv','') for f in files if '.csv' in f]))
    available_dates.sort()
    return available_dates


dt=st.selectbox("Date of analysis",get_all_available_dates())
etf_holdings_df=get_etf_holdings_data(dt)
etf_sector_weights=get_etf_sector_weights(etf_holdings_df)

# Show the plot using streamlit
st.plotly_chart(etf_sector_heatmap(etf_sector_weights))


etf_name=st.selectbox("ETF Name:",etf_holdings_df.etf_name.sort_values().unique().tolist())
rel_cols=st.multiselect(
    "Data Field:",
    etf_holdings_df.columns.tolist(),
    ['Date','Ticker','Name','Sector','Weight (%)','Price','Market Value']
)
st.dataframe(
    etf_holdings_df
    .query(f"etf_name == '{etf_name}' ")
    .sort_values("Weight (%)", ascending=False)
    .reindex(rel_cols,axis=1)
    .reset_index(drop=True)
)
