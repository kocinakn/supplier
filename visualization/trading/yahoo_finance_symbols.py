import matplotlib.ticker as plticker
import mplfinance as mpf
from tools import data_preparation as dp
import time
import yfinance as yf
import warnings
import datetime
from tools import enumeration
warnings.filterwarnings("ignore")


def data_for_time_thresholds(symbol, time_thresholds, candle_intervals):
    df1 = yf.download(tickers=yahoo_finance_api_symbols_dictionary(symbol), start=(datetime.datetime.now() - datetime.timedelta(days=int(time_thresholds[0][:-1]))),
                      end=datetime.datetime.now(), interval=candle_intervals[0])

    df2 = yf.download(tickers=yahoo_finance_api_symbols_dictionary(symbol), start=(datetime.datetime.now() - datetime.timedelta(days=int(time_thresholds[1][:-1]))),
                      end=datetime.datetime.now(), interval=candle_intervals[1])

    df3 = yf.download(tickers=yahoo_finance_api_symbols_dictionary(symbol), start=(datetime.datetime.now() - datetime.timedelta(days=int(time_thresholds[2][:-1]))),
                      end=datetime.datetime.now(), interval=candle_intervals[2])

    df4 = yf.download(tickers=yahoo_finance_api_symbols_dictionary(symbol), start=(datetime.datetime.now() - datetime.timedelta(days=int(time_thresholds[3][:-1]))),
                      end=datetime.datetime.now(), interval=candle_intervals[3])
    time.sleep(3)
    return df1, df2, df3, df4


def check_if_datetime_index_is_native(df1, df2, df3, df4):
    df1 = dp.convert_index_to_native(df1)
    df2 = dp.convert_index_to_native(df2)
    df3 = dp.convert_index_to_native(df3)
    df4 = dp.convert_index_to_native(df4)
    return df1, df2, df3, df4


def plot_indexes(symbols):
    for symbol in symbols:
        intervals = enumeration.yahoo_finance_major_indices_intervals()
        try:
            time_thresholds = list(intervals.keys())
            candle_intervals = list(intervals.values())
            df1, df2, df3, df4 = data_for_time_thresholds(symbol, time_thresholds, candle_intervals)

            df1, df2, df3, df4 = check_if_datetime_index_is_native(df1, df2, df3, df4)

            fig = mpf.figure(figsize=(18, 15))
            fig.subplots_adjust(hspace=0.2)
            fig.suptitle(symbol)

            mc = mpf.make_marketcolors(base_mpf_style='yahoo')
            s = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc)
            ax1 = fig.add_subplot(221)
            ax2 = fig.add_subplot(222)
            ax3 = fig.add_subplot(223)
            ax4 = fig.add_subplot(224)

            mpf.plot(df1,
                     type='candle',
                     ax=ax1,
                     style=s,
                     axtitle=f'{time_thresholds[0]}: {candle_intervals[0]}. Start: {df1.index[0]}',
                     vlines=dict(vlines=df2.index[0], linestyle='-.',
                                 colors='brown'),
                     xrotation=5,
                     show_nontrading=False)
            ax1.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df1) / 5)))

            mpf.plot(df2,
                     type='candle',
                     ax=ax2,
                     style=s,
                     axtitle=f'{time_thresholds[1]}: {candle_intervals[1]}. Start: {df2.index[0]}',
                     vlines=dict(vlines=df3.index[0], linestyle='-.',
                                 colors='brown'),
                     xrotation=5,
                     show_nontrading=False)
            ax2.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df2) / 5)))

            mpf.plot(df3,
                     type='candle',
                     ax=ax3,
                     style=s,
                     axtitle=f'{time_thresholds[2]}: {candle_intervals[2]}. Start: {df3.index[0]}',
                     vlines=dict(vlines=df4.index[0], linestyle='-.',
                                 colors='brown'),
                     xrotation=5,
                     show_nontrading=False)
            ax3.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df3) / 5)))

            mpf.plot(df4,
                     type='candle',
                     ax=ax4,
                     style=s,
                     axtitle=f'{time_thresholds[3]}: {candle_intervals[3]}. Start: {df4.index[0]}',
                     xrotation=5,
                     show_nontrading=False)
            ax4.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df4) / 5)))
            mpf.show()
        except IndexError:
            continue
        except ValueError:
            continue
    return


def yahoo_finance_api_symbols_dictionary(symbol):
    d = {'USDPLN': 'PLN=X',
         'NASDAQ': '^IXIC',
         'US500': '^GSPC',
         'DAX': '^GDAXI',
         'GOLD': 'GC=F',
         'SILVER': 'SI=F',
         'DXY': 'DX-Y.NYB',
         'US10Y': '^TNX',
         'BRENT': 'BZ=F',
         'WTI': 'CL=F'
         }
    try:
        return d[symbol]
    except KeyError:
        return symbol


if __name__ == '__main__':
    tickers = ['WTI', 'BRENT']
    plot_indexes(tickers)
