import numpy as np
import pandas as pd
import datetime
import holidays


def get_method_names(obj):
    return [attr for attr in dir(obj) if callable(getattr(obj, attr))]


def convert_to_float(x, multiplier=1.0, default=np.nan):
    """
    Converts the input value to a float, applying a multiplier if provided.

    Args:
        x: The value to be converted.
        multiplier: Optional. The multiplier to be applied to the converted value. Default is 1.0.
        default: Optional. The default value to be returned if the conversion fails. Default is np.nan.

    Returns:
        The converted float value, multiplied by the multiplier if provided. If the conversion fails, returns the default value.
    """
    if isinstance(x, float):
        return x * multiplier
    elif isinstance(x, int):
        return float(x) * multiplier
    elif isinstance(x, str):
        # remove bad values and convert to float np.nan
        if x in ['—','nan','none','null','n/a','N/A','None','NULL','NaN','NA']:
            return default
        try:
            return float(x.replace(',','')) * multiplier
        except Exception as e:
            print(f'Failed to convert {x} to float: {e}')
            return default


def check_group_by_input(func):
    def wrapper(data, groupby, vars, weight, reset_index=True):
        import warnings
        warnings.filterwarnings("ignore")
        if not isinstance(data, pd.DataFrame):
            raise ValueError("data must be a pandas DataFrame")
        if not isinstance(groupby, list):
            raise ValueError("groupby must be a list")
        if not isinstance(vars, list):
            raise ValueError("vars must be a list")
        if not isinstance(reset_index, bool):
            raise ValueError("reset_index must be a bool")
        if len(groupby) == 0:
            raise ValueError("groupby must have at least one element")
        if len(vars) == 0:
            raise ValueError("vars must have at least one element")
        if not set(groupby).issubset(set(data.columns)):
            raise ValueError("groupby must be a subset of data's columns")
        if not set(vars).issubset(set(data.columns)):
            raise ValueError("vars must be a subset of data's columns")
        if weight is not None:
            if not set([weight]).issubset(set(data.columns)):
                raise ValueError("weight must be a subset of data's columns")
        if len(set(groupby).intersection(set(vars))) > 0:
            raise ValueError("groupby and vars must not have any common elements")
        return func(data, groupby, vars, weight, reset_index)

    return wrapper


def assign_market_cap_group(x):
    """Assign a market cap group based on market cap value in millions of dollars"""
    if np.isnan(x) or (x is None) or (not isinstance(x, float)):
        return 'N/A'
    if x >= 200000:
        return "Mega"
    elif 10000 <= x < 200000:
        return "Large"
    elif 2000 <= x < 10000:
        return "Mid"
    elif 300 <= x < 2000:
        return "Small"
    elif 50 <= x < 300:
        return "Micro"
    elif 0 < x < 50:
        return "Nano"
    else:
        return "N/A"
    

def get_previous_trading_day(date):
    """
    Returns the previous trading day before the given date.

    Parameters:
    date (str or datetime): The date for which to find the previous trading day.

    Returns:
    str: The previous trading day in ISO format (YYYY-MM-DD).
    """
    date = pd.to_datetime(date)
    import holidays
    us_holidays = [pd.to_datetime(dt) for dt in holidays.US(years=date.year).keys()]
    while True:
        date = date - pd.Timedelta(days=1)
        day = date.day_of_week
        if day < 5 and date not in us_holidays:
            return date.date().isoformat()