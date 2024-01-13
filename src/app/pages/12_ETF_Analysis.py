import sys
import itertools
from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent.parent
sys.path.append(str(ROOT_DIR))

from src.utils.pandas_utils import df_filter, set_cols_numeric
from src.data.equity_data.tradingview import TradingView
from src.data.equity_data.yfinance_old import Stock
from src.config import DB_DIR

import streamlit as st
import plotly.express as px


import sqlite3
import datetime
import pandas as pd
conn = sqlite3.connect(DB_DIR/'etf_holdings.db')
query = """
select *
from etf_holdings
where etf_ticker = 'LABU'
"""
df=pd.read_sql(query,conn)
conn.close()

st.write(df[['security_name','weight']].assign(weight=lambda x: x['weight']*100))
