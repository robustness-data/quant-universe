import streamlit as st
import pandas as pd
import plotly.express as px


from pathlib import Path
import os
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent

# Load data
df = pd.read_csv(ROOT_DIR/"data/equity_market/1_etf/ishares_holdings_2023-06-27.csv")

# Convert the 'Weight (%)' column to numeric, handling any errors that might arise
df['Weight (%)'] = pd.to_numeric(df['Weight (%)'], errors='coerce')

# Group by 'Sector' and 'etf_ticker' and calculate the total weight
grouped_df = df.groupby(['Sector', 'etf_name'])['Weight (%)'].sum().reset_index()

# Pivot the data for the heatmap
pivot_df = grouped_df.pivot(columns='Sector', index='etf_name', values='Weight (%)')

# Create heatmap
fig = px.imshow(pivot_df,
                labels=dict(x="Sector", y="ETF Name", color="Weight (%)"),
                x=pivot_df.columns, y=pivot_df.index,
                text_auto=True, aspect='auto')

fig.update_layout(
    title_text="<b>Sector Weights of iShares ETFs</b>",  # Title in bold
    title_x=0,  # Align title to center
    autosize=True,  # The graph will resize itself to the size of the container
    height=500, width=800  # You can adjust the height to your preference
)

# Show the plot using streamlit
st.plotly_chart(fig)
