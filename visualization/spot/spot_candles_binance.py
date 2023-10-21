from tools import data_preparation as dp
import matplotlib.pyplot as plt
import matplotlib.ticker
import mplfinance as mpf
import pandas as pd
import warnings
pd.options.display.float_format = '{:20,.5f}'.format

plt.style.use('ggplot')
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', None)
warnings.filterwarnings("ignore")


def plot_binance_spot_candles(tick, intervals):
    candlesticks_data = dp.binance_spot_candles(tick, intervals)
    for days, candles in candlesticks_data.items():
        if candles is None:
            continue
        support_and_resistance_lines = dp.get_support_and_resistance(candles)

        min_and_max_price = dp.candles_min_and_max_price(candles)

        horizontal_lines_colors = dp.horizontal_lines_colors(support_and_resistance_lines, min_and_max_price)

        candles['volume_usd'] = dp.volume_usd_column_for_ohlc(candles)
        candles['vol_mean'] = candles.volume.mean()

        lowest_volume = candles.sort_values('volume', ascending=True).head(5)

        ap = [mpf.make_addplot(candles['vol_mean'], panel=1, type='line', color='brown'),
              mpf.make_addplot(candles['volume_usd'], panel=1, type='scatter', markersize=5, color='black',
                               secondary_y=True, ylabel='USD')]

        mc = mpf.make_marketcolors(up='white', down='#000000', volume='#483D8B')
        s = mpf.make_mpf_style(base_mpl_style='seaborn', marketcolors=mc)

        fig_title = dp.fig_title_for_spot_with_one_ticker(tick, candles, support_and_resistance_lines)

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
                             panel_ratios=(3, 1),
                             returnfig=True,
                             show_nontrading=False)

        axes[2].get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
        axes[3].get_yaxis().set_major_formatter(matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))

        for price in support_and_resistance_lines:
            axes[0].text(0, price, f'{price}', fontsize=12, bbox=dict(facecolor='blue', alpha=0.25),
                         horizontalalignment='right', verticalalignment='center')
        for pr in min_and_max_price:
            axes[0].text(len(candles), pr, f'{pr}', fontsize=12, bbox=dict(facecolor='k', alpha=0.25),
                         horizontalalignment='left', verticalalignment='center')
        mpf.show()
    return


if __name__ == '__main__':
    # interval = None
    # interval = {'90d': '4h', '30d': '1h'}
    #interval = {'90d': '4h', '30d': '4h', '3d': '15m'}
    interval = {'180d': '4h', '90d': '4h'}

    symbols = ['STGUSDT']
    for symbol in symbols:
        plot_binance_spot_candles(symbol, interval)
