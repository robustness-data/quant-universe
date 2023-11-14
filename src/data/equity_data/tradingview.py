import pandas as pd
import src.config as cfg
from src.data.database.db_manager import TradingViewDB
from src.utils.pandas_utils import df_filter, set_cols_numeric

import logging
logger = logging.getLogger(__name__)


tv_univ_label_to_keys = {
    'US All Universe': 'us',
    'NASDAQ Composite': 'nasdaq-composite',
    'NASDAQ 100': 'ndx',
    'NASDAQ Golden Dragon China': 'nasdaq-china',
    'NASDAQ Biotech': 'nasdaq-biotech',
    'S&P 500': 'sp500',
    'Russell 1000': 'r1k',
    'Russell 2000': 'r2k',
    'Russell 3000': 'r3k',
    'China All Universe': 'china',
    'Hong Kong Universe': 'hongkong'
}
tv_univ_keys_to_labels = {v: k for k, v in tv_univ_label_to_keys.items()}


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


class TradingView:

    def __init__(self):
        self.database = TradingViewDB()
        self.data_df = pd.read_parquet(cfg.TV_CACHE_DIR/'tradingview_data.parquet')
        self.univ_label_to_keys = tv_univ_label_to_keys
        self.univ_keys_to_labels = tv_univ_keys_to_labels
        self.dates = self.data_df.Date.unique().tolist()
        self.varnames = self.data_df.columns.tolist()
        self.category_dict = category_dict
        self.sector_industry_map = self.data_df.groupby(['Sector'])\
            .apply(lambda x: x.Industry.unique().tolist()).to_dict()

        # create a dictionary of unique values for universe, sector, and industry by date
        temp_dict = {k: None for k in ['Universe','Sector','Industry']}
        for k, v in temp_dict.items():
            if v is None:
                v = self.data_df.groupby(['Date'])[k].unique().to_dict()
                setattr(self, k.lower()+'_by_date', v)

    def load_available_dates(self):
        pass

    def load_data_dt(self, dt):
        """
        Load the data for a specific date.
        :param dt: the date of analysis
        :return:
        """
        data_dt = df_filter(df=self.data_df, filter_dict={'Date': dt})
        available_univ = data_dt.Universe.unique().tolist()
        available_univ_labels = [self.univ_keys_to_labels.get(x, x) for x in available_univ]
        return {
            'data_dt': data_dt,
            'available_univ_labels': available_univ_labels
        }


if __name__ == "__main__":

    tv = TradingView()

    #print(tv.universe_list)
    #print(tv.dates)
    #print(tv.varnames)
    #print(tv.sector_names)
    #print(tv.industry_names)
    #print(tv.sector_industry_map)

    #tv.database.drop_table('universe')  # TODO: fix issues here
    #tv.database.create_universe_table()
    #tv.database.populate_universe_table(universe_df=tv.data_df)