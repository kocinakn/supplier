from tools import data_preparation as dp
import matplotlib.pyplot as plt
import mplfinance as mpf
import pandas as pd
import warnings
pd.options.display.float_format = '{:20,.5f}'.format

plt.style.use('ggplot')
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', None)
warnings.filterwarnings("ignore")


def plot_coins_with_reference(coins, ref_coin, intervals):
    for c in coins:
        source_candles = dp.spot_candles_for_coin(c, intervals)
        ref_candles = dp.spot_candles_for_coin(ref_coin, intervals)
        for k, v in source_candles.items():
            reference_candles = ref_candles[k]
            threshold_date = max([v.index[0], reference_candles.index[0]])

            v = v[v.index > threshold_date]
            reference_candles = reference_candles[reference_candles.index > threshold_date]

            ap = [mpf.make_addplot(reference_candles, type='candle', ylabel=f'{ref_coin}', panel=1)]

            fig_title = dp.fig_title_for_spot_with_spot_reference(c, ref_coin, v)
            mpf.plot(v,
                     type="candle",
                     title=fig_title,
                     addplot=ap,
                     style='classic',
                     panel_ratios=(1, 1),
                     volume_panel=2,
                     scale_width_adjustment=dict(candle=1.25),
                     ylabel=f'{c}')
    return


if __name__ == '__main__':
    #interval = None
    interval = {'365d': '1d', '30d': '4h', '3d': '15m'}
    cryptocurrencies = ['OP']
    reference_coin = 'ETH'
    plot_coins_with_reference(cryptocurrencies, reference_coin, interval)
