import datetime
import os, sys
import random
import sqlite3
import streamlit as st
import pandas as pd
import plotly.express as px
from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
import src.config as cfg

# ---------------------------------------- #
# US Treasury auction data
# ---------------------------------------- #

# load the data

TREASURY_CACHE_DIR = cfg.MACRO_CACHE_DIR/'USTreasury'
ustsy_db_path = TREASURY_CACHE_DIR/'us_treasury.db'
# connect to the database
conn = sqlite3.connect(ustsy_db_path)

# show tables
#cursor = conn.cursor()
#cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
#st.write(cursor.fetchall())

df = pd.read_sql_query("SELECT * FROM auction_investor_allotment", conn)
df['issue_date'] = df['issue_date'].apply(lambda x: pd.to_datetime(x).date())
df['maturity_date'] = df['maturity_date'].apply(lambda x: pd.to_datetime(x).date())
df['ttm'] = [ (y-x)/datetime.timedelta(days=1) for x, y in zip(df.issue_date, df.maturity_date)]
df['ttm_year'] = df['ttm']/365.0
df['ttm_year_bucket'] = df['ttm_year'].apply(lambda x: f"{int(x)}-{int(x)+1}Y")
df['ttm_year_bucket_5year'] = df['ttm_year'].apply(lambda x: f"{int(x/5)*5}-{int(x/5)*5+5}Y")

participants = [
      "Banks",
      "Broker Dealers",
      "Fed",
      "Foreign",
      "Individuals",
      "Investment Fund",
      "Other",
      "Pension",
      'total_issue'
]


#st.write( (df['maturity_date'].iloc[0] - df['issue_date'].iloc[0])/pd.Timedelta(days=1) )

#st.write(df)

#st.write(
#    df.groupby(['ttm_year_bucket_5year', 'issue_date'])\
#        ['total_issue'].sum().reset_index()\
#        .pivot(columns='ttm_year_bucket_5year', index='issue_date', values='total_issue')
#)


def get_issuance_by_investor(investor: str, freq: str):
    issuance_by_investor = df.groupby(['description', 'issue_date'])\
            [investor].sum().reset_index()\
            .pivot(columns='description', index='issue_date', values=investor)\
            .drop(columns=[x for x in df['description'].unique() if 'CMB' in x])\
            .fillna(0.0).rename(pd.to_datetime).resample(freq).sum()
    return issuance_by_investor

monthly_total_issue = get_issuance_by_investor('total_issue', 'M')
#st.write(monthly_total_issue)

# plot bar chart for monthly_total_issue using plotly
#fig = px.bar(monthly_total_issue.reset_index(), x='issue_date', y=monthly_total_issue.columns.tolist())
#st.plotly_chart(fig)



