import os, sys
from pathlib import Path
import datetime
import itertools
from tqdm import tqdm
import pandas as pd
import streamlit as st
from src.data.db_manager import TradingViewDB
import src.config as cfg

import logging
logger = logging.getLogger(__name__)


univ_names_renamer = {
    'US All Universe': 'us',
    'NASDAQ Composite': 'nasdaq-composite',
    'NASDAQ 100': 'nasdaq-100',
    'NASDAQ Golden Dragon China': 'nasdaq-china',
    'NASDAQ Biotech': 'nasdaq-biotech',
    'S&P 500': 'sp500',
    'Russell 1000': 'r1k',
    'Russell 2000': 'r2k',
    'Russell 3000': 'r3k',
    'China All Universe': 'china',
    'Hong Kong Universe': 'hongkong'
}


category_dict = {
    "General Meta": [
            'Ticker', 'Description', 'Sector', 'Industry', 'Country',
            'Number of Employees', 'Total Shares Outstanding', 'Shares Float',
            "Price", 'Market Capitalization', 'Money Flow (14)',
            "Recent Earnings Date", "Upcoming Earnings Date", "Technical Rating"
        ],
    "Performance": [
        'Change', 'Change %', "Change from Open %", 'Change 1W, %', 'Change 1M, %',
        'Weekly Performance', "Monthly Performance", '3-Month Performance', '6-Month Performance',
        'Yearly Performance', '5Y Performance', 'YTD Performance',
        '1-Month High', '3-Month High', '6-Month High', '52 Week High', 'All Time High',
        '1-Month Low', '3-Month Low', '6-Month Low', '52 Week Low', 'All Time Low',
        "1-Year Beta"
    ],
    "Trend": [
        'Exponential Moving Average (5)',
        'Exponential Moving Average (10)',
        'Exponential Moving Average (20)',
        'Exponential Moving Average (30)',
        'Exponential Moving Average (50)',
        'Exponential Moving Average (100)',
        'Simple Moving Average (5)',
        'Simple Moving Average (10)',
        'Simple Moving Average (20)',
        'Simple Moving Average (30)',
        'Simple Moving Average (50)',
        'Simple Moving Average (100)',
        'Simple Moving Average (200)',
    ],
    "Risk": [
        'Volatility', 'Volatility Week', 'Volatility Month'
    ],
    "Volume": [
        'Volume', 'Volume*Price', 'Volume Weighted Average Price',  'Volume Weighted Moving Average (20)',
        'Average Volume (10 day)', 'Average Volume (30 day)', 'Average Volume (60 day)', 'Average Volume (90 day)',
    ],
    "Balance Sheet": [
        'Total Liabilities (FY)', 'Cash & Equivalents (FY)', 'Cash and short term investments (FY)',
        'Total Assets (MRQ)', 'Total Current Assets (MRQ)', 'Cash & Equivalents (MRQ)', 'Cash and short term investments (MRQ)',
        'Total Liabilities (MRQ)', 'Total Debt (MRQ)', 'Net Debt (MRQ)',
        'Enterprise Value (MRQ)',
        'Debt to Equity Ratio (MRQ)','Quick Ratio (MRQ)', 'Current Ratio (MRQ)',

        'Total Debt (Annual YoY Growth)',
        'Total Debt (Quarterly YoY Growth)',
        'Total Debt (Quarterly QoQ Growth)',
        'Total Assets (Annual YoY Growth)',
        'Total Assets (Quarterly YoY Growth)',
        'Total Assets (Quarterly QoQ Growth)'
    ],
    "Profitability": [
        'Gross Margin (FY)', 'Net Margin (FY)', 'Free Cash Flow Margin (FY)', 'Operating Margin (FY)',
        'Gross Margin (TTM)', 'Net Margin (TTM)', 'Free Cash Flow Margin (TTM)', 'Operating Margin (TTM)',
        'Pretax Margin (TTM)',
        'Return on Assets (TTM)', 'Return on Equity (TTM)', 'Return on Invested Capital (TTM)',
    ],
    "Income Statement": [
        'Total Revenue (FY)', 'Gross Profit (FY)', 'Net Income (FY)', 'Dividends Paid (FY)',
        'EBITDA (TTM)', 'Basic EPS (TTM)', 'EPS Diluted (TTM)',
        'Revenue per Employee (FY)', 'Dividends per Share (FY)', 'EPS Diluted (FY)', 'Basic EPS (FY)',
        'EPS Forecast (MRQ)', 'EPS Diluted (MRQ)', "Dividends per Share (MRQ)", 'Gross Profit (MRQ)',

        'Revenue (Annual YoY Growth)', 'EPS Diluted (Annual YoY Growth)', 'EBITDA (Annual YoY Growth)',
        'Free Cash Flow (Annual YoY Growth)', 'Gross Profit (Annual YoY Growth)', 'Net Income (Annual YoY Growth)',
        "Dividends per share (Annual YoY Growth)",

        'Revenue (Quarterly QoQ Growth)', 'EPS Diluted (Quarterly QoQ Growth)', 'EBITDA (Quarterly QoQ Growth)',
        'Free Cash Flow (Quarterly QoQ Growth)', 'Gross Profit (Quarterly QoQ Growth)', 'Net Income (Quarterly QoQ Growth)',

        'Revenue (Quarterly YoY Growth)', 'EPS Diluted (Quarterly YoY Growth)', 'EBITDA (Quarterly YoY Growth)',
        'Free Cash Flow (Quarterly YoY Growth)', 'Gross Profit (Quarterly YoY Growth)', 'Net Income (Quarterly YoY Growth)',

        'Revenue (TTM YoY Growth)', 'EPS Diluted (TTM YoY Growth)', 'EBITDA (TTM YoY Growth)',
        'Free Cash Flow (TTM YoY Growth)', 'Gross Profit (TTM YoY Growth)', 'Net Income (TTM YoY Growth)',
    ],
    'Investment':[
        'Research & development Ratio (TTM)', 'Research & development Ratio (FY)',
    ],
    "Valuation": [
        'Price to Book (FY)', 'Price to Book (MRQ)', 'Price to Sales (FY)', "Dividend Yield Forward",
        'Price to Earnings Ratio (TTM)', 'Enterprise Value/EBITDA (TTM)',
        'Price to Free Cash Flow (TTM)', 'Price to Revenue Ratio (TTM)',
    ],

}


class TradingViewFundamental:

    def __init__(self):
        self.universe_db = TradingViewDB()
        #self.load_stock_universe_data()
        #self.univ_names_renamer = univ_names_renamer
        #self.category_dict = category_dict
        pass

    def get_universe_meta(self, universe_df):
        self.univ_names = universe_df.Universe.unique().tolist()
        self.univ_dates = universe_df.Date.unique().tolist()
        self.univ_fundamentals=universe_df.columns.tolist()
        self.sector_names=universe_df.Sector.sort_values().unique().tolist()
        self.industry_names=universe_df.Industry.sort_values().unique().tolist()
        self.sector_industry_map = universe_df.groupby(['Sector']).apply(lambda x: x.Industry.unique().tolist()).to_dict()
        self.universe_df = universe_df

    def write_universe_to_db(self):
    
        for f in tqdm(os.listdir(cfg.CACHE_DIR/'raw'), desc="Writing TradingView stock universe to database"):
            print(f)
            index, as_of_date = f.replace('.csv','').split('_')
            univ_df = pd.read_csv(cfg.CACHE_DIR / 'raw' / f).assign(Universe=index).assign(Date=as_of_date)
            self.universe_db.
            #for i, row in univ_df.iterrows():
            #    universe_db.create_table_and_insert_data('tv_universe', row.to_dict())
            del univ_df

        universe_db.close_connection()

    def load_universe_from_db(self):    
        return pd.read_sql('select TOP (100) * from tv_universe', TradingViewDB('equity_universe.db', cfg.DB_DIR).conn)

    def load_universe_from_csv(self):

        univ_list = []
        for f in os.listdir(cfg.CACHE_DIR/'raw'):
            print(f)
            index, as_of_date = f.replace('.csv','').split('_')
            try:
                univ_df = pd.read_csv(cfg.CACHE_DIR/'raw'/f, on_bad_lines='warn').assign(Universe=index).assign(Date=as_of_date)
                univ_list.append(univ_df)
                del univ_df
            except Exception as e:
                logging.error(f'Error in reading {f}: {e}')
        universe_df = pd.concat(univ_list)
        universe_df = universe_df.dropna(subset='Market Capitalization')


if __name__ == "__main__":


    tv_fund = TradingViewFundamental()
    tv_fund.write_universe_to_db()
    print(tv_fund.load_universe_from_db().to_string())