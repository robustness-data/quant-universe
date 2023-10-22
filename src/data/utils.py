import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots


def get_method_names(obj):
    return [attr for attr in dir(obj) if callable(getattr(obj, attr))]


def universe_filter(df, filter_dict):
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

# Candlestick chart
def plot_candlestick_chart(df):
        
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03, ratio_y=0.8)
    fig.add_trace(
        go.Candlestick(
            x=df['Date'], 
            open=df['Open'], 
            high=df['High'],
            low=df['Low'], 
            close=df['Close']), 
        row=1, col=1
    )

    fig.add_trace(go.Bar(x=df['Date'], y=df['Volume']), row=2, col=1)

    # Date slider
    #fig.update_xaxes(rangeslider_visible=True)

    # Update layout
    fig.update_layout(title="Historical Price", yaxis_title='Price', yaxis2_title='Volume')

    return fig


# OHLC chart
def plot_ohlc_chart(df):
    
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03)
    fig.add_trace(
        go.Ohlc(
            x=df['Date'], 
            open=df['Open'], 
            high=df['High'],
            low=df['Low'], 
            close=df['Close']), 
        row=1, col=1
    )
    fig.add_trace(go.Bar(x=df['Date'], y=df['Volume']), row=2, col=1)
    fig.update_xaxes(rangeslider_visible=True)
    fig.update_layout(title="Historical Price", yaxis_title='Price', yaxis2_title='Volume')

    return fig


# Line chart
def plot_line_chart(df):
        
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03)
    fig.add_trace(
        go.Scatter(x=df['Date'], y=df['Close'], mode='lines'), row=1, col=1)
    fig.add_trace(go.Bar(x=df['Date'], y=df['Volume']), row=2, col=1)
    #fig.update_xaxes(rangeslider_visible=True)
    fig.update_layout(title="Historical Price", yaxis_title='Price', yaxis2_title='Volume')

    return fig