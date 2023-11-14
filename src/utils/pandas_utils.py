import pandas as pd


def df_filter(df, filter_dict=None):
    if filter_dict is None:
        return df
    df = df.copy()
    for column, value in filter_dict.items():
        if value is not None:
            if isinstance(value, list) or isinstance(value, tuple):
                df = df[df[column].isin(value)]
            else:
                df = df[df[column] == value]
    return df


def set_cols_numeric(df, cols):
    for col in cols:
        df[col] = df[col].astype(float)
    return df
