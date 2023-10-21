from tools import data_preparation as dp
from tools import connections
from tools import enumeration
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker_matplotlib
import mplfinance as mpf
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:20,.5f}'.format
plt.style.use('ggplot')


def format_y_axes(axes):
    axes[1].get_yaxis().set_major_formatter(ticker_matplotlib.FuncFormatter(lambda x, p: format(int(x), ',')))
    axes[2].get_yaxis().set_major_formatter(ticker_matplotlib.FuncFormatter(lambda x, p: format(int(x), ',')))
    axes[3].get_yaxis().set_major_formatter(ticker_matplotlib.FuncFormatter(lambda x, p: format(int(x), ',')))
    return


def plot_open_interest(ticker, candles, open_interest):
    candles['volume_usd'] = dp.volume_usd_column_for_ohlc(candles)
    open_interest, candles = dp.cut_frames_regarding_date(open_interest, candles)
    ap = [
        mpf.make_addplot(open_interest.sumOpenInterest, type='line', ylabel=f'{ticker} Open Interest', y_on_right=True),
        mpf.make_addplot(candles['volume_usd'], panel=1, type='scatter', markersize=3, color='blue', secondary_y=True,
                         ylabel='USD (scatter)')]

    fig_title = dp.fig_title_for_futures_open_interest(ticker, open_interest)

    fig, axes = mpf.plot(candles,
                         type="candle",
                         volume=True,
                         title=fig_title,
                         addplot=ap,
                         style='classic',
                         panel_ratios=(4, 1),
                         scale_width_adjustment=dict(candle=1.25),
                         returnfig=True)
    format_y_axes(axes)
    mpf.show()
    return


def plot_open_interests(ticker, oi_intervals=None):
    if oi_intervals is None:
        oi_intervals = enumeration.binance_futures_oi_intervals()
    client = connections.connect_binance_futures_api()
    oi = dp.binance_futures_open_interest(ticker, oi_intervals, client)
    candlesticks = dp.binance_futures_candles(ticker, oi_intervals)
    for days, open_interest in oi.items():
        candles = candlesticks[days]
        plot_open_interest(ticker, candles, open_interest)
    return


if __name__ == '__main__':
    #intervals = None
    intervals = {'30d': '4h', '14d': '15m'}
    ticks = ['LTCUSDT']
    for t in ticks:
        plot_open_interests(t, intervals)
