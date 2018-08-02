import plotly.graph_objs as go
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd
import numpy as np
import datetime
import time
from dash.dependencies import Output, Input
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.schema import MetaData
from datetime import timedelta
from app import app
from dateutil.relativedelta import relativedelta, FR
from datetime import datetime as dt
from datetime import datetime
import plotly

engine = create_engine('postgresql://stocks:stocks@localhost:2345/option_price_tracking')
connection = engine.connect()

meta = MetaData()
meta.reflect(bind=engine)
contract_timestamp_table = meta.tables["contract_ib_first_timestamp"]
contracts = meta.tables["contracts"]

has_color = 'rgba(76, 175, 80,1.0)'
hasnt_color = 'rgba(255, 193, 7,1.0)'
cant_color = 'rgba(156, 39, 176,1.0)'
no_timestamp_color = 'rgba(255, 87, 34,1.0)'


def get_points(my_data, symbol, right, selected_date):
    has_marker = {'color': f'{has_color}', 'size': 5, 'symbol': 'square'}
    hasnt_marker = {'color': f'{hasnt_color}', 'size': 5, 'symbol': 'square'}

    trace = [
        go.Candlestick(
            x=my_data.index,
            open=my_data.open,
            high=my_data.high,
            low=my_data.low,
            close=my_data.close,
            name='Daily Bars',

        ),
        go.Bar(
            x=my_data.index,
            y=my_data.volume,
            name='Daily Volume'
        )

    ]

    return {
        'data': trace,
        'layout': go.Layout(
            title=f'Daily bars - {selected_date.strftime("%Y%M%d")} {symbol} {right} ',
            showlegend=True,
            legend=go.Legend(
                x=0,
                y=1.0
            ),
            margin=go.Margin(l=100, r=40, t=40, b=30),
        )
    }


def get_data(right, symbol, expiry_date):
    query = 'select c.*, b.* ' \
            'from contracts c left join contract_daily_bars b on c."conId" = b."conId" ' \
            'where c.strike = 180 and c.symbol=\'{}\' and c.right=\'{}\' and  c."lastTradeDateOrContractMonth"::date = \'{}\' ' \
            'order by b.date '.format(symbol, right, expiry_date)

    con_df = pd.read_sql(query, connection, parse_dates={"date": {"utc": True}})
    con_df.strike = con_df.strike.astype(float)
    con_df = con_df.set_index('date')
    df = con_df[['open', 'high', 'low', 'close', 'volume']]
    ohlc_dict = {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'}
    df = df.resample('1D').apply(ohlc_dict).dropna(how='any')
    return df


def get_symbols():
    query = 'select distinct symbol from contracts order by symbol'

    symbols = pd.read_sql(query, connection)

    return [{'label': symbol, 'value': symbol} for symbol in symbols.symbol]


layout = html.Div(className='container',
                  children=[
                      html.Nav(className='navbar navbar-expand-lg navbar-light bg-light nav-tabs nav-fill', children=[
                          html.A('Timestamps By Date', href='/apps/contract_timestamps',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Timestamps By Symbol', href='/apps/contract_timestamps_by_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Timestamps By Date and Symbol', href='/apps/contract_timestamps_by_date_and_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars', href='/apps/daily_bars',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars By Date', href='/apps/daily_bars_by_date',
                                 className='nav-item nav-link btn  btn-outline-success'),
                          html.A('Daily Bars By Date And Symbol', href='/apps/daily_bars_by_date_and_symbol',
                                 className='nav-item nav-link btn btn-outline-success'),
                          html.A('Daily Bars By Symbol And Strike', href='/apps/daily_bars_by_symbol_and_strike',
                                 className='nav-item nav-link btn active btn-outline-success'),
                      ]),
                      html.Div([
                          html.Label("Expiry Date:", htmlFor="date-picker", className='form-check-label'),
                          html.Div([
                              dcc.DatePickerSingle(
                                  id='date-picker',
                                  min_date_allowed=dt(2018, 6, 15),
                                  max_date_allowed=dt(2030, 12, 31),
                                  initial_visible_month=dt.now(),
                                  date=dt.now() + relativedelta(weekday=FR(+1)),
                              ),

                          ], className='form-check'),
                      ], className='form-check-inline col-auto'),
                      html.Div([
                          html.Label("Symbol:", htmlFor="symbol", className='form-check-label'),
                          dcc.Dropdown(
                              id='symbol',
                              options=get_symbols(),
                              value='QQQ',
                              className='form-check-inline form-check-input',
                              clearable=False
                          ),

                      ], className='form-check-inline col-2'),
                      html.Div([
                          html.Label("Right:", htmlFor='right', className='form-check-label'),
                          dcc.RadioItems(
                              id='right',
                              options=[
                                  {'label': 'Calls', 'value': 'C'},
                                  {'label': 'Puts', 'value': 'P'},
                              ],
                              value='C',
                              labelClassName='radio-inline',
                              inputClassName='radio form-check-input',
                              className=''
                          )

                      ], className='form-check-inline col-auto'),

                      dcc.Graph(
                          style={'height': 500},
                          id='my-bar-candle',
                          className='col',
                      ),
                      dcc.Interval(
                          id='interval-component',
                          interval=600 * 1000,  # in milliseconds
                          n_intervals=0
                      ),

                  ])


@app.callback(Output('my-bar-candle', 'figure'),
              [Input('right', 'value'),
               Input('symbol', 'value'),
               Input('interval-component', 'n_intervals'),
               Input('date-picker', 'date')])
def update_bars_figure(right, symbol, n_intervals, date):
    con_df = get_data(right, symbol, date)
    my_points = get_points(con_df, symbol, right, dt.strptime(date.split(" ")[0], '%Y-%M-%d'))
    fig = plotly.tools.make_subplots(rows=2, cols=1, vertical_spacing=0.2, shared_xaxes=True, shared_yaxes=True)
    fig['layout'] = my_points['layout']
    fig.append_trace(trace=my_points['data'][0], row=2, col=1)
    fig.append_trace(trace=my_points['data'][1], row=1, col=1)
    return fig
