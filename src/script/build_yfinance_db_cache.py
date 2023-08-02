import os, sys, logging
from pathlib import Path

logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
    logging.info(f"Added {str(ROOT_DIR)} to sys.path")

import src.config as cfg
from src.data.equity_api_yfinance import Stock
from src.data.db_manager import DBManager


def populate_quarterly_income_stmt(asset, db_manager):
    input_df = (
        asset.quarterly_income_stmt
        .stack().rename('value').reset_index()
        .rename(columns={'level_0': 'income_item', 'level_1': 'date'})
        .assign(ticker=asset.ticker)
    )

    logger.info(f"Populating quarterly income statement for {asset.ticker}")
    db_manager.create_table_from_df(table_name='quarterly_income_stmt', df=input_df)
    db_manager.insert_data_from_df(table_name='quarterly_income_stmt', df=input_df)
    db_manager.cursor.close()


def populate_quarterly_balance_sheet(asset, db_manager):
    input_df = (
        asset.quarterly_balance_sheet
        .stack().rename('value').reset_index()
        .rename(columns={'level_0': 'balance_sheet_item', 'level_1': 'date'})
        .assign(ticker=asset.ticker)
    )

    logger.info(f"Populating quarterly balance sheet for {asset.ticker}")
    db_manager.create_table_from_df(table_name='quarterly_balance_sheet', df=input_df)
    db_manager.insert_data_from_df(table_name='quarterly_balance_sheet', df=input_df)
    db_manager.cursor.close()


def main():
    print(str(cfg.DB_DIR/'yfinance'))
    db_manager = DBManager(db_name=str(cfg.DB_DIR/'yfinance'))

    asset = Stock.load_stock('RIVN')
    asset.history(period='max')
    asset.get_financials()
    
    populate_quarterly_income_stmt(asset, db_manager)
    populate_quarterly_balance_sheet(asset, db_manager)

    print(db_manager.query_data_into_df(table_name='quarterly_income_stmt', columns=['*']).to_string())
    db_manager.cursor.close()
    print(db_manager.query_data_into_df(table_name='quarterly_balance_sheet', columns=['*']).to_string())
    db_manager.cursor.close()


if __name__ == "__main__":

    main()