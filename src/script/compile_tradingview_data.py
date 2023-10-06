import os
import datetime
import itertools
from tqdm import tqdm
import pandas as pd

from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent
CACHE_DIR=ROOT_DIR/'data'/'equity_market'/'3_fundamental'
print("Cache directory:", CACHE_DIR)


def compile_tradingview_data():
    file_list = [f for f in os.listdir(CACHE_DIR/'raw') if '.csv' in f]
    file_list.sort()
    file_list.remove('_db_setup_universe.csv')
    data_list = []
    for f in tqdm(file_list, desc="Compiling TradingView data ..."):
        index, as_of_date = f.replace('.csv', '').split('_')
        try:
            data_df = pd.read_csv(CACHE_DIR/'raw'/f).assign(Universe=index).assign(Date=as_of_date)
            data_list.append(data_df)
            del data_df
        except Exception as e:
            print("Failed to read data from %s: %s", f, e)
    all_data_df = pd.concat(data_list)
    all_data_df = all_data_df.dropna(subset='Market Capitalization')
    all_data_df.astype(str).to_parquet(CACHE_DIR/'tradingview_data.parquet')


if __name__ == '__main__':

    compile_tradingview_data()