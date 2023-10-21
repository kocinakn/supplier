from tools import data_preparation as dp
from tools import enumeration
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
pd.options.display.float_format = '{:20,.5f}'.format
plt.style.use('ggplot')


def decide_if_plot_non_trading(ref):
    if 'MT4' in ref:
        return False
    else:
        return True


def get_reference_candles(ref, interval):
    if 'MT4' in ref:
        candlesticks = dp.mt5_candles(ref.rsplit('MT4_')[1], interval)
    else:
        candlesticks = dp.binance_futures_candles(ref, interval)
    return candlesticks


def plot_futures_with_reference(ticker, reference, interval=None):
    if interval is None:
        interval = enumeration.binance_futures_candlesticks_intervals()
    source_candlesticks = dp.binance_futures_candles(ticker, interval)
    for ref in reference:
        reference_candlesticks = get_reference_candles(ref, interval)
        for days, candles in source_candlesticks.items():
            ref_candles = reference_candlesticks[days]

            plot_non_trading = decide_if_plot_non_trading(ref)
            if plot_non_trading is False:
                candles = candles[candles.index.dayofweek < 5]
            ref_candles, candles = dp.cut_frames_regarding_date(ref_candles, candles)

            ap = [mpf.make_addplot(ref_candles, type='candle', ylabel=f'{ref}', panel=1)]

            fig_title = dp.fig_title_for_futures_with_reference(ticker, ref, candles, plot_non_trading)

            mpf.plot(candles,
                     type="candle",
                     title=fig_title,
                     addplot=ap,
                     style='classic',
                     panel_ratios=(4, 4),
                     volume_panel=2,
                     scale_width_adjustment=dict(candle=1.25),
                     show_nontrading=plot_non_trading,
                     ylabel=f'{ticker}')
    return


if __name__ == '__main__':
    tickers = ['BTCUSDT']

    references = ['MT4_US500', 'MT4_USDPLN']
    # references = ['LTCUSDT']

    intervals = {'90d': '4h', '30d': '4h', '7d': '1h'}

    for t in tickers:
        plot_futures_with_reference(t, references, intervals)
