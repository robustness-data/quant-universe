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


class Stocks(Tickers):

    def __init__(self, tickers: list):
        """
        Utilize yfinance.Tickers to fetch multiple stocks data.
        :param tickers: a list of tickers
        """
        super().__init__(tickers=tickers)
        self.tickers = []




class Portfolio:

    def __init__(self, owner):
        self.owner = owner
        self.holdings = pd.DataFrame(columns=["ticker", "shares"])
        self.transaction_history = pd.DataFrame(columns=["ticker", "shares", "transaction_type", "date"])

    def add_position(self, ticker, shares, date):
        if ticker in self.holdings["ticker"].values:
            self.holdings.loc[self.holdings["ticker"] == ticker, "shares"] += shares
        else:
            self.holdings = self.holdings.append({"ticker": ticker, "shares": shares}, ignore_index=True)
        self.transaction_history = self.transaction_history.append(
            {"ticker": ticker, "shares": shares, "transaction_type": "Buy", "date": date}, ignore_index=True)

    def sell_position(self, ticker, shares, date):
        if ticker in self.holdings["ticker"].values:
            self.holdings.loc[self.holdings["ticker"] == ticker, "shares"] -= shares
            if self.holdings.loc[self.holdings["ticker"] == ticker, "shares"].values[0] <= 0:
                self.holdings = self.holdings[self.holdings["ticker"] != ticker]
            self.transaction_history = self.transaction_history.append(
                {"ticker": ticker, "shares": shares, "transaction_type": "Sell", "date": date}, ignore_index=True)
        else:
            print("Position not exist.")

    def fetch_holdings(self):
        return self.holdings

    def fetch_transaction_history(self):
        return self.transaction_history

    def fetch_portfolio_value(self):
        value = 0
        for index, row in self.holdings.iterrows():
            value += Stock(row["ticker"]).fetch_price() * row["shares"]
        return value

    def fetch_portfolio_return(self):
        returns = pd.Series(dtype=float)
        for index, row in self.transaction_history.iterrows():
            stock_return = Stock(row["ticker"]).fetch_price(period="1y").pct_change().dropna()
            if len(returns) == 0:
                returns = stock_return
            else:
                returns = returns.add(stock_return, fill_value=0)
        return returns