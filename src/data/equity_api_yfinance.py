import pandas as pd
import streamlit as st
from yfinance import Ticker, Tickers
import quantstats as qs


class Stock(Ticker):

    def __init__(self, ticker):
        super().__init__(ticker)
        self.ticker = ticker

    @st.cache_data
    @staticmethod
    def load_stock(ticker):
        return Stock(ticker)