import os
import datetime
import itertools
from tqdm import tqdm
import pandas as pd

from pathlib import Path
print(__file__)
ROOT_DIR=Path(__file__).parent.parent.parent
CACHE_DIR=ROOT_DIR/'data'/'equity_market'/'3_fundamental'
print("Cache directory for stock universe:", CACHE_DIR)


def compile_stock_universe_data():
    univ_list = []
    for f in tqdm(os.listdir(CACHE_DIR/'raw'), desc="Compiling stock universe data ..."):
        index, as_of_date = f.replace('.csv','').split('_')
        univ_df = pd.read_csv(CACHE_DIR/'raw'/f).assign(index_name=index).assign(as_of_date=as_of_date)
        univ_list.append(univ_df)
        del univ_df
    all_univ_df = pd.concat(univ_list)
    all_univ_df = all_univ_df.dropna(subset='Market Capitalization')
    for c in tqdm(all_univ_df.columns, desc="Converting variables to string"):
        all_univ_df[c] = all_univ_df[c].astype(str)
    return all_univ_df


def main():
    all_univ_df = compile_stock_universe_data()
    all_univ_df.to_parquet(CACHE_DIR/'universe.parquet')


if __name__ == '__main__':

    main()