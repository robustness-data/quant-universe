import numpy as np


def get_method_names(obj):
    return [attr for attr in dir(obj) if callable(getattr(obj, attr))]


def convert_to_float(x, multiplier=1.0, default=np.nan):
    if isinstance(x, float):
        return x * multiplier
    elif isinstance(x, int):
        return float(x) * multiplier
    elif isinstance(x, str):
        try:
            return float(x.replace(',','')) * multiplier
        except Exception as e:
            print(f'Failed to convert {x} to float: {e}')
            return default