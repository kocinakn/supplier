from tools import data_preparation as dp
import MetaTrader5 as mt5
import datetime
import pandas as pd


def binance_spot_candlesticks_intervals(client):
    return {'1825d': client.KLINE_INTERVAL_1WEEK,
            '600d': client.KLINE_INTERVAL_1DAY,
            '365d': client.KLINE_INTERVAL_1DAY,
            '182d': client.KLINE_INTERVAL_1DAY,
            '90d': client.KLINE_INTERVAL_4HOUR}


def binance_futures_candlesticks_intervals():
    timedelta = (datetime.datetime.now() - pd.to_datetime('2019-12-31')).days
    return {f'{str(timedelta)}d': '1w',
            '600d': '1d',
            '365d': '1d',
            '182d': '1d',
            '90d': '4h'}


def kucoin_spot_candlesticks_intervals():
    return {'1825d': '1week',
            '600d': '1day',
            '365d': '1day',
            '182d': '1day',
            '90d': '4hour'}


def gateio_spot_candlesticks_intervals():
    return {'1825d': '1w',
            '600d': '1d',
            '365d': '1d',
            '182d': '1d',
            '90d': '4h'}


def bithumb_candlesticks_intervals():
    return {'1825d': "24H",
            '600d': '24H',
            '365d': "24H",
            '182d': "6H",
            '90d': "6H",
            '30d': "1H"}


def huobi_spot_candlesticks_intervals():
    return {'1825d': ['1day', '1825'],
            '600d': ['1day', '600'],
            '365d': ['1day', '365'],
            '182d': ['4hour', '1092'],
            '90d': ['4hour', '550'],
            '30d': ['60min', '720']}


def mexc_spot_candlesticks_intervals():
    return {'990d': "1d",
            '600d': "1d",
            '365d': "1d",
            '182d': "1d",
            '90d': "4h",
            '30d': "60m",
            '14d': "60m"}


def coingecko_spot_candlesticks_intervals():
    return {'max': '4d',
            '365d': '4d',
            '180d': '4d',
            '30d': '4h',
            '1d': '30m'}


def binance_futures_oi_intervals():
    return {'30d': '4h',
            '14d': '1h',
            '7d': '1h',
            '1d': '5m'}


def watchlist_intervals():
    return {'30d': '4h',
            '7d': '30m',
            '3d': '30m',
            '1d': '15m'}


def binance_futures_watchlist_intervals():
    return {'90d': '1d',
            '30d': '4h',
            '7d': '1h',
            '3d': '15m'}


def get_candle_type_for_mt5(candle_interval):
    d = {'1w': mt5.TIMEFRAME_W1,
         '1d': mt5.TIMEFRAME_D1,
         '4h': mt5.TIMEFRAME_H4,
         '1h': mt5.TIMEFRAME_H1,
         '15m': mt5.TIMEFRAME_M15,
         '5m': mt5.TIMEFRAME_M5,
         '1m': mt5.TIMEFRAME_M1}
    return d[candle_interval]


def intervals_for_mt5_tickers():
    return {'600d': '1w',
            '365d': '1d',
            '90d': '4h',
            '30d': '1h'}


def yahoo_finance_major_indices_intervals():
    return {'1825d': '1wk', '365d': '1d', '90d': '1d', '30d': '1h'}