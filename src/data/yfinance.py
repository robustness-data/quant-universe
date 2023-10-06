import io
import requests
import csv
import time
import sqlite3
import concurrent.futures

import yfinance as yf
import pandas as pd
import numpy as np

# TODO: to be cleaned up and refactored into other modules

class Stock:
    def __init__(self, ticker):
        self.ticker = ticker
        self.yf_ticker = yf.Ticker(ticker)
        self.meta_info = self.get_meta_info() or {}
        self.historical_data = None
        self.option_chain_data = self.get_option_chain()
        self.set_additional_attributes()

    def get_meta_info(self):
        try:
            return self.yf_ticker.info
        except Exception as e:
            print(f"Error fetching meta info for {self.ticker}: {e}")
            return None

    def set_additional_attributes(self):
        """
        This function sets additional attributes for the stock object.
        """
        self.company_name = self.meta_info.get('longName', '')
        self.country = self.meta_info.get('country', '')
        self.currency = self.meta_info.get('currency', '')
        self.industry = self.meta_info.get('industry', '')
        self.market_cap = self.meta_info.get('marketCap', '')
        self.price_to_earnings = self.meta_info.get('trailingPE', '')
        self.shares_outstanding = self.meta_info.get('sharesOutstanding', '')

    def get_historical_data(self, start_date, end_date, interval='1d'):
        """
        This function downloads the historical price and volume data for the stock for the given date range and interval.
        """
        self.historical_data = self.yf_ticker.history(start=start_date, end=end_date, interval=interval)

    def get_option_chain(self):
        """
        This function returns the option chain data for the stock.
        """
        option_chain_data = {}
        try:
            expirations = self.yf_ticker.options
            for expiration_date in expirations:
                option_chain_data[expiration_date] = self.yf_ticker.option_chain(expiration_date)
            return option_chain_data
        except Exception as e:
            print(f"Error fetching option chain for {self.ticker}: {e}")
            return None

    def get_financial_statements(self, freq='annual'):
        """
        This function returns the financial statements for the stock for the given frequency.
        """
        return {
            'balance_sheet': self.yf_ticker.balance_sheet(freq),
            'cashflow': self.yf_ticker.cashflow(freq),
            'earnings': self.yf_ticker.earnings(freq)
        }

    def calculate_sma(self, window=20):
        """
        This function calculates the Simple Moving Average for the stock for the given window.
        """
        if self.historical_data is None:
            print("Historical data not available. Please download historical data first.")
            return None

        sma = self.historical_data['Close'].rolling(window=window).mean()
        return sma

    def calculate_ema(self, window=20):
        """
        This function calculates the Exponential Moving Average for the stock for the given window.
        """
        if self.historical_data is None:
            print("Historical data not available. Please download historical data first.")
            return None

        ema = self.historical_data['Close'].ewm(span=window).mean()
        return ema

    def get_put_call_ratios(self):
        """
        This function calculates the Put/Call ratio for the stock.
        """
        put_call_ratios = pd.Series(index=self.option_chain_data.keys())

        for expiration_date, option_chain in self.option_chain_data.items():
            calls = option_chain.calls
            puts = option_chain.puts

            call_volume = calls['volume'].sum()
            put_volume = puts['volume'].sum()

            if call_volume == 0:
                put_call_ratios[expiration_date] = np.nan
            else:
                put_call_ratios[expiration_date] = put_volume / call_volume

        return put_call_ratios


class StockUniverse:
    def __init__(self):
        self.stocks = {}

    def get_stock(self, ticker):
        """
        This function returns the stock object for the given ticker. If the stock object is not present in the universe,
        it creates a new stock object and adds it to the universe.
        """
        return self.stocks.get(ticker) or self.add_stock(ticker)

    def add_stock(self, ticker):
        """
        This function creates a new stock object for the given ticker and adds it to the universe.
        """
        stock = Stock(ticker)
        self.stocks[ticker] = stock
        return stock

    def populate_universe(self, tickers):
        """
        This function adds the given list of tickers to the universe.
        """
        for ticker in tickers:
            self.add_stock(ticker)

    def get_put_call_ratios_dataframe(self, tickers):
        """
        This function returns a dataframe containing the Put/Call ratio for each stock in the given list of tickers.
        """
        put_call_ratios_data = {}
        for ticker in tickers:
            stock = self.get_stock(ticker)
            put_call_ratios = stock.get_put_call_ratios()
            put_call_ratios_data[ticker] = put_call_ratios

        put_call_ratios_df = pd.DataFrame(put_call_ratios_data)
        return put_call_ratios_df


class OptionCache:
    def __init__(self, tickers, interval=1):
        self.universe = StockUniverse()
        self.stocks = [self.universe.get_stock(ticker) for ticker in tickers]
        self.interval = interval
        self.option_chains = {}
        self.create_db()

    def create_db(self):
        self.conn = sqlite3.connect("option_cache.db")
        cursor = self.conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS option_chains (
                timestamp INTEGER,
                ticker TEXT,
                expiration_date TEXT,
                option_type TEXT,
                contractSymbol TEXT,
                lastTradeDate INTEGER,
                strike REAL,
                lastPrice REAL,
                bid REAL,
                ask REAL,
                change REAL,
                percentChange REAL,
                volume INTEGER,
                openInterest INTEGER,
                impliedVolatility REAL,
                inTheMoney INTEGER,
                contractSize TEXT,
                currency TEXT
            )
        """)
        self.conn.commit()

    def save_option_chain_to_db(self, timestamp, ticker, expiration_date, option_type, option_chain):
        cursor = self.conn.cursor()

        for index, row in option_chain.iterrows():
            row_data = (timestamp, ticker, expiration_date, option_type) + tuple(row)
            cursor.execute("""
                INSERT INTO option_chains (timestamp, ticker, expiration_date, option_type, contractSymbol, lastTradeDate, strike, lastPrice, bid, ask, change, percentChange, volume, openInterest, impliedVolatility, inTheMoney, contractSize, currency)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, row_data)

        self.conn.commit()

    def fetch_and_save_option_chain(self, stock):
        try:
            option_chains = stock.get_option_chain()
            timestamp = int(time.time())

            for expiration_date, option_chain in option_chains.items():
                if option_chain is not None:
                    self.option_chains[(stock.ticker, expiration_date)] = option_chain
                    self.save_option_chain_to_db(timestamp, stock.ticker, expiration_date, 'call', option_chain.calls)
                    self.save_option_chain_to_db(timestamp, stock.ticker, expiration_date, 'put', option_chain.puts)
        except Exception as e:
            print(f"Error fetching and saving option chain data for {stock.ticker}: {e}")

    def fetch_option_chains_for_multiple_stocks(self):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.map(self.fetch_and_save_option_chain, self.stocks)

    def start_periodic_fetch(self):
        while True:
            try:
                self.fetch_option_chains_for_multiple_stocks()
                time.sleep(self.interval)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"Error during periodic fetch: {e}")

    def close_db(self):
        self.conn.close()


if __name__ == '__main__':

    tickers = ['AAPL', 'MSFT', 'GOOG']

    universe = StockUniverse()

    # Add stocks to the universe
    apple = universe.add_stock("AAPL")
    microsoft = universe.add_stock("MSFT")

    # Download historical price and volume data
    apple.get_historical_data(start_date='2020-1-1', end_date='2023-5-27')

    # test
    option_cache = OptionCache(tickers=tickers, interval=60)
    try:
        option_cache.start_periodic_fetch()
    except KeyboardInterrupt:
        print("Stopping the periodic fetch...")
    finally:
        option_cache.close_db()

