from tools import data_preparation as dp

import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
pd.options.display.float_format = '{:20,.5f}'.format
plt.style.use('ggplot')


def plot_mt5_data(ticker, show_weeks: bool, interval=None):
    candlesticks = dp.mt5_candles(ticker, interval)
    for days, candles in candlesticks.items():
        support_and_resistance_lines = dp.get_support_and_resistance(candles, 3)
        min_and_max_price = dp.candles_min_and_max_price(candles)
        horizontal_lines_colors = dp.horizontal_lines_colors(support_and_resistance_lines, min_and_max_price)

        fig_title = dp.fig_title_for_mt5_with_one_ticker(candles, ticker, support_and_resistance_lines, show_weeks)

        fig, axes = mpf.plot(candles,
                             type="candle",
                             title=fig_title,
                             style='charles',
                             scale_width_adjustment=dict(candle=1.25),
                             hlines=dict(hlines=support_and_resistance_lines + min_and_max_price,
                                         linestyle='--', colors=horizontal_lines_colors, linewidths=0.1),
                             ylabel=f'{ticker}',
                             volume=True,
                             returnfig=True,
                             show_nontrading=show_weeks)
        for price in support_and_resistance_lines:
            axes[0].text(0, round(price, 4), f'{round(price, 4)}', fontsize=12, bbox=dict(facecolor='blue', alpha=0.25),
                         horizontalalignment='right', verticalalignment='center')
        for pr in min_and_max_price:
            axes[0].text(len(candles), pr, f'{pr}', fontsize=12, bbox=dict(facecolor='k', alpha=0.25),
                         horizontalalignment='left', verticalalignment='center')
        mpf.show()
    return candlesticks


if __name__ == '__main__':
    # mt5 does not return weekends - last 30 days means last 30 trading days
    intervals = {'90d': '4h', '3d': '15m'}
    tickers = ['USDPLN']
    show_weekends = False
    for t in tickers:
        plot_mt5_data(t, show_weekends, intervals)


