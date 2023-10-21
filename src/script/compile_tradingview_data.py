import os, datetime, itertools, zipfile, tqdm
from io import BytesIO
from tqdm import tqdm
import pandas as pd

from pathlib import Path
import src.config as cfg
print("Cache directory:", cfg.TV_CACHE_DIR)


def _read_zip_file(file_name):
    with zipfile.ZipFile(file_name, 'r') as zip_ref:
        output = dict()
        for filename in zip_ref.namelist():
            if filename.endswith('.csv') and ('MACOSX' not in filename):
                with zip_ref.open(filename) as file:
                    try:
                        df = pd.read_csv(BytesIO(file.read()))
                        output[filename] = df
                    except Exception as e:
                        print(f"Failed to read data from {filename}: {e}", )
                        continue
        return output


def compile_tradingview_data():
    """
    Compile all tradingview data into a single parquet file. Warning: the compiled file is too large for Git push.
    Try only use for local purposes.
    :return:
    """

    file_list = [f for f in os.listdir(cfg.TV_CACHE_DIR/'raw') if '.zip' in f]
    file_list.sort()
    data_list = []
    for f in tqdm(file_list, desc="Compiling TradingView data ..."):
        print(f)
        output = _read_zip_file(cfg.TV_CACHE_DIR/'raw'/f)
        for filename, df in output.items():
            print(filename.replace('.csv', '').split('_'))
            index, as_of_date = filename.replace('.csv', '').split('_')
            try:
                data_list.append(df.assign(Universe=index).assign(Date=as_of_date))
                del df
            except Exception as e:
                print("Failed to append data from %s: %s", filename, e)
    all_data_df = pd.concat(data_list)
    all_data_df = all_data_df.dropna(subset='Market Capitalization')
    all_data_df.astype(str).to_parquet(cfg.TV_CACHE_DIR/'tradingview_data.parquet')


if __name__ == '__main__':

    compile_tradingview_data()