from plotly import graph_objects as go
from plotly.subplots import make_subplots


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


def plot_line_chart(df):

    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.03)
    fig.add_trace(
        go.Scatter(x=df['Date'], y=df['Close'], mode='lines'), row=1, col=1)
    fig.add_trace(go.Bar(x=df['Date'], y=df['Volume']), row=2, col=1)
    #fig.update_xaxes(rangeslider_visible=True)
    fig.update_layout(title="Historical Price", yaxis_title='Price', yaxis2_title='Volume')

    return fig
