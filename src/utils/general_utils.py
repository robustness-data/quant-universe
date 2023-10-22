def get_method_names(obj):
    return [attr for attr in dir(obj) if callable(getattr(obj, attr))]
