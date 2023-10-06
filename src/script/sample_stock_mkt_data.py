import os, sys, logging, time, datetime
from pathlib import Path
logger = logging.getLogger(__name__)

ROOT_DIR = Path(__file__).parent.parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))
    logging.info(f"Added {str(ROOT_DIR)} to sys.path")

import src.config as cfg
from src.data.equity_api_yfinance import Stock
from src.data.db_manager import DBManager
from sqlalchemy import Column
from sqlalchemy import Integer, String, Float, DateTime

import numpy as np
import pandas as pd


def sample_current_price(asset, db_manager):
    """
    Sample current price and insert into database.
    :param asset:
    :param db_manager:
    :return:
    """
    current_time=datetime.datetime.now()
    current_price = asset.get_info().get('currentPrice', np.nan)
    if np.isnan(current_price):
        logger.warning(f"Current price for {asset.ticker} is not available.")
    input_df = pd.DataFrame({'ticker': [asset.ticker], 'datetime': [current_time], 'price': [current_price]})
    db_manager.insert_data_from_df(table_name='market_price', df=input_df)


def main():
    print(str(cfg.DB_DIR / 'yfinance'))
    db_manager = DBManager(db_name=str(cfg.DB_DIR / 'yfinance'))
    columns = [
        Column('ticker', db_manager.dtype_map.get('str'), primary_key=True),
        Column('datetime', db_manager.dtype_map.get('datetime'), primary_key=True),
        Column('price', db_manager.dtype_map.get('float'))
    ]
    db_manager.create_table(table_name='market_price', columns=columns)

    asset = Stock.load_stock('RIVN')
    logger.info(f"Successfully loaded {asset.ticker} from yfinance.")

    # for every 30 seconds, run sample_current_price
    while True:
        try:
            sample_current_price(asset, db_manager)
            db_manager.cursor.close()
            logger.info(f"Successfully sampled current price for {asset.ticker}")
        except Exception as e:
            logger.exception(f"Failed to sample current price for {asset.ticker}: {e}")

        time.sleep(30)


if __name__ == "__main__":
    main()