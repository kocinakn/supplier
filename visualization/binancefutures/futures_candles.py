from tools import data_preparation as dp
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker_matplotlib
import mplfinance as mpf
import pandas as pd
import warnings
warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:20,.5f}'.format
plt.style.use('ggplot')


def plot_exchange_candle(ticker, candles, return_fig):
    if candles is None:
        return

    mc = mpf.make_marketcolors(up='white', down='#000000', volume='#483D8B')
    s = mpf.make_mpf_style(base_mpl_style='seaborn', marketcolors=mc)

    support_and_resistance_lines = dp.get_support_and_resistance(candles)
    min_and_max_price = dp.candles_min_and_max_price(candles)
    horizontal_lines_colors = dp.horizontal_lines_colors(support_and_resistance_lines, min_and_max_price)

    fig_title = dp.fig_title_for_futures_with_one_ticker(ticker, candles, support_and_resistance_lines)

    if candles.volume.sum() != 0:
        candles['volume_usd'] = dp.volume_usd_column_for_ohlc(candles)
        candles['vol_mean'] = candles.volume.mean()
        lowest_volume = candles.sort_values('volume', ascending=True).head(5)

        ap = [mpf.make_addplot(candles['vol_mean'], panel=1, type='line', color='brown'),
              mpf.make_addplot(candles['volume_usd'], panel=1, type='scatter', markersize=5, color='k',
                               secondary_y=True, ylabel='USD (scatter)')]
        fig, axes = mpf.plot(candles,
                             type="candle",
                             title=fig_title,
                             style=s,
                             volume=True,
                             addplot=ap,
                             vlines=dict(vlines=list(lowest_volume.index), linestyle='--', colors='brown'),
                             scale_width_adjustment=dict(candle=1.25, volume=0.7),
                             hlines=dict(hlines=support_and_resistance_lines + min_and_max_price, linestyle='-',
                                         colors=horizontal_lines_colors, linewidths=0.01),
                             panel_ratios=(2, 1),
                             returnfig=True,
                             show_nontrading=False)
        axes[2].get_yaxis().set_major_formatter(ticker_matplotlib.FuncFormatter(lambda x, p: format(int(x), ',')))
        axes[3].get_yaxis().set_major_formatter(ticker_matplotlib.FuncFormatter(lambda x, p: format(int(x), ',')))
    else:
        fig, axes = mpf.plot(candles,
                             type="candle",
                             title=fig_title,
                             style=s,
                             volume=False,
                             scale_width_adjustment=dict(candle=1.25),
                             hlines=dict(hlines=support_and_resistance_lines, linestyle='--', colors='k',
                                         linewidths=0.03),
                             returnfig=True,
                             show_nontrading=False)
    for price in support_and_resistance_lines:
        axes[0].text(0, price, f'{price}', fontsize=12, bbox=dict(facecolor='blue', alpha=0.25),
                     horizontalalignment='right', verticalalignment='center')
    for pr in min_and_max_price:
        axes[0].text(len(candles), pr, f'{pr}', fontsize=12, bbox=dict(facecolor='k', alpha=0.25),
                     horizontalalignment='left', verticalalignment='center')
    if return_fig:
        return fig
    mpf.show()
    return


def plot_exchange_candles(ticker, interval, return_fig=False):
    candlesticks_data = dp.binance_futures_candles(ticker, interval)
    for days, candles in candlesticks_data.items():
        plot_exchange_candle(ticker, candles, return_fig)
    return


if __name__ == '__main__':
    ticks = ['ETHUSDT']
    #intervals = {'30d': '4h'}
    intervals = {'30d': '4h', '1d': '30m'}
    for t in ticks:
        plot_exchange_candles(t, intervals)
