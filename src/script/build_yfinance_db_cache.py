import sys, logging
from tqdm import tqdm
from pathlib import Path

logger = logging.getLogger('build_yfinance_db_cache')

ROOT_DIR = Path(__file__).parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
    logging.info(f"Added {str(ROOT_DIR)} to sys.path")

from data.equity_data.yfinance import Stock
from data.database.db_manager import DBManager

import pandas as pd


def populate_quarterly_income_stmt(asset, db_manager):
    input_df = (
        asset.quarterly_income_stmt
        .stack().rename('value').reset_index()
        .rename(columns={'level_0': 'income_item', 'level_1': 'date'})
        .assign(ticker=asset.ticker)
        .assign(date=lambda x: pd.to_datetime(x['date']).apply(lambda y: y.date()))
        .dropna(subset=['date'])
    )

    logger.info(f"Populating quarterly income statement for {asset.ticker}")
    db_manager.create_table_from_df(table_name='quarterly_income_stmt', df=input_df)
    db_manager.insert_data_from_df(table_name='quarterly_income_stmt', df=input_df)


def populate_quarterly_balance_sheet(asset, db_manager):
    input_df = (
        asset.quarterly_balance_sheet
        .stack().rename('value').reset_index()
        .rename(columns={'level_0': 'balance_sheet_item', 'level_1': 'date'})
        .assign(ticker=asset.ticker)
        .assign(date=lambda x: pd.to_datetime(x['date']).apply(lambda y: y.date()))
        .dropna(subset=['date'])
    )

    logger.info(f"Populating quarterly balance sheet for {asset.ticker}")
    db_manager.create_table_from_df(table_name='quarterly_balance_sheet', df=input_df)
    db_manager.insert_data_from_df(table_name='quarterly_balance_sheet', df=input_df)


def populate_historical_price(asset, db_manager):

    input_df = asset.history(period='max').assign(ticker=asset.ticker).rename(lambda x: pd.to_datetime(x).date())
    input_df.rename(columns=lambda x: x.lower().replace(' ', '_'), inplace=True)
    input_df.index.name = 'date'
    input_df = input_df.reset_index()
    logger.info(f"Populating historical price for {asset.ticker}")
    db_manager.create_table_from_df(table_name='historical_price', df=input_df)
    db_manager.insert_data_from_df(table_name='historical_price', df=input_df)


def populate_asset_data(ticker:str, db_manager:DBManager):
    asset = Stock.load_stock(ticker)
    asset.history(period='max')
    asset.get_financials()
    populate_quarterly_income_stmt(asset, db_manager)
    populate_quarterly_balance_sheet(asset, db_manager)
    populate_historical_price(asset, db_manager)


def main():
    # suppress logging from sqlalchemy
    logging.getLogger('db_manager').setLevel(logging.ERROR)
    logging.getLogger('build_yfinance_db_cache').setLevel(logging.WARNING)

    # clear the db cache
    DBManager.drop_db(db_name='yfinance')

    # create database
    db_manager = DBManager(db_name='yfinance')

    # load universe from trading view data
    us_universe_df = pd.read_csv(ROOT_DIR / 'data' / 'equity_market' / '3_fundamental' / 'raw' / 'us_2023-07-28.csv')
    ch_universe_df = pd.read_csv(ROOT_DIR / 'data' / 'equity_market' / '3_fundamental' / 'raw' / 'china_2023-07-28.csv')
    hk_universe_df = pd.read_csv(ROOT_DIR / 'data' / 'equity_market' / '3_fundamental' / 'raw' / 'hongkong_2023-07-21.csv')

    universe_spec = {
        'us': us_universe_df.Ticker.unique().tolist(),
        'china': ch_universe_df.Ticker.unique().tolist(),
        'hongkong': hk_universe_df.Ticker.unique().tolist()
    }

    for mkt, universe_tics in universe_spec.items():
        for tic in tqdm(universe_tics, desc=f"Populating {mkt} universe"):
            try:
                populate_asset_data(tic, db_manager)
            except Exception as e:
                logger.exception(f"Failed to populate data for {tic}: {e}")


if __name__ == "__main__":

    main()