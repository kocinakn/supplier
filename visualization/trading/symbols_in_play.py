from tools import data_preparation as dp
from postgres import modifications, queries
from postgres import executions
from tools import enumeration
import matplotlib.pyplot as plt
import mplfinance as mpf
import numpy as np
import matplotlib.ticker as plticker
import datetime
import warnings
warnings.filterwarnings("ignore")


def dataframes_from_dict(candles):
    return candles['90d'], candles['30d'], candles['7d'], candles['3d']


def fig_titles_for_for_symbols(df1, df2, df3, df4, sr1, sr2, sr3, sr4, ticker):
    t1 = dp.fig_title_for_spot_with_one_ticker(ticker, df1, sr1, True)
    t2 = dp.fig_title_for_spot_with_one_ticker(ticker, df2, sr2, True)
    t3 = dp.fig_title_for_spot_with_one_ticker(ticker, df3, sr3, True)
    t4 = dp.fig_title_for_spot_with_one_ticker(ticker, df4, sr4, True)
    return t1, t2, t3, t4


def support_and_resistance_lines_for_symbols(df1, df2, df3, df4):
    s1 = dp.get_support_and_resistance(df1)
    s2 = dp.get_support_and_resistance(df2)
    s3 = dp.get_support_and_resistance(df3)
    s4 = dp.get_support_and_resistance(df4)
    return s1, s2, s3, s4


def min_and_max_prices_for_symbols(df1, df2, df3, df4):
    p1 = dp.candles_min_and_max_price(df1)
    p2 = dp.candles_min_and_max_price(df2)
    p3 = dp.candles_min_and_max_price(df3)
    p4 = dp.candles_min_and_max_price(df4)
    return p1, p2, p3, p4


def colors_for_lines_for_symbols(sr1, sr2, sr3, sr4, p1, p2, p3, p4):
    c1 = dp.horizontal_lines_colors(sr1, p1)
    c2 = dp.horizontal_lines_colors(sr2, p2)
    c3 = dp.horizontal_lines_colors(sr3, p3)
    c4 = dp.horizontal_lines_colors(sr4, p4)
    return c1, c2, c3, c4


def plot_symbols(candles, row, return_fig=False):
    df1, df2, df3, df4 = dataframes_from_dict(candles)

    sr1, sr2, sr3, sr4 = support_and_resistance_lines_for_symbols(df1, df2, df3, df4)

    title_1, title_2, title_3, title_4 = fig_titles_for_for_symbols(df1, df2, df3, df4, sr1, sr2, sr3, sr4,
                                                                    row["ticker"])

    p1, p2, p3, p4 = min_and_max_prices_for_symbols(df1, df2, df3, df4)

    c1, c2, c3, c4 = colors_for_lines_for_symbols(sr1, sr2, sr3, sr4, p1, p2, p3, p4)

    fig = mpf.figure(figsize=(18, 15))
    fig.subplots_adjust(hspace=0.2)
    fig.suptitle(f'{row["date"]}. {row["ticker"]}. OI: {dp.string_number_from_integer(int(row["oi_usd"]))}, '
                 f'turnover: {dp.string_number_from_integer(int(row["turnover"]))}.')

    mc = mpf.make_marketcolors(base_mpf_style='yahoo')
    s = mpf.make_mpf_style(base_mpf_style='nightclouds', marketcolors=mc)
    vp_color = 'grey'

    ax1 = fig.add_subplot(221)
    ax2 = fig.add_subplot(222)
    ax3 = fig.add_subplot(223)
    ax4 = fig.add_subplot(224)

    bucket_size1 = ((max(df1['high']) - min(df1['high'])) / np.ceil(np.sqrt(len(df1)))) * 0.5
    volprofile1 = df1['volume'].groupby(df1['high'].apply(lambda x: bucket_size1 * round(x / bucket_size1, 0))).sum()
    try:
        mpf.plot(df1,
                 type='candle',
                 ax=ax1,
                 style=s,
                 axtitle=title_1 + f' Bins: {len(volprofile1)}',
                 hlines=dict(hlines=sr1 + p1, linestyle='-',
                             colors=c1, linewidths=0.01),
                 vlines=dict(vlines=datetime.datetime.now() - datetime.timedelta(days=30), linestyle='-.', colors='brown'),
                 xrotation=5)
    except ValueError:
        mpf.plot(df1,
                 type='candle',
                 ax=ax1,
                 style=s,
                 axtitle=title_1 + f' Bins: {len(volprofile1)}',
                 hlines=dict(hlines=sr1 + p1, linestyle='-',
                             colors=c1, linewidths=0.01),
                 xrotation=5)
    try:
        ax1.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df1)/5)))
    except ValueError:
        pass

    for price in sr1:
        ax1.text(0, price, f'{price}', fontsize=12, bbox=dict(facecolor='blue', alpha=0.25),
                 horizontalalignment='right', verticalalignment='center')
    for pr in p1:
        ax1.text(len(df1), pr, f'{pr}', fontsize=12, bbox=dict(facecolor='k', alpha=0.25),
                 horizontalalignment='left', verticalalignment='center')
    vpax1 = fig.add_axes(ax1.get_position())
    vpax1.set_axis_off()
    vpax1.set_xlim(right=1.2 * max(volprofile1.values))
    vpax1.barh(volprofile1.keys().values, volprofile1.values, height=bucket_size1, align='center', color=vp_color,
               alpha=0.45)

    bucket_size2 = ((max(df2['high']) - min(df2['high'])) / np.ceil(np.sqrt(len(df2)))) * 0.5
    volprofile2 = df2['volume'].groupby(df1['high'].apply(lambda x: bucket_size2 * round(x / bucket_size2, 0))).sum()
    try:
        mpf.plot(df2,
                 type='candle',
                 ax=ax2,
                 style=s,
                 axtitle=title_2 + f' Bins: {len(volprofile2)}',
                 hlines=dict(hlines=sr2 + p2, linestyle='-',
                             colors=c2, linewidths=0.01),
                 vlines=dict(vlines=datetime.datetime.now() - datetime.timedelta(days=7), linestyle='-.', colors='brown'),
                 xrotation=5)
    except ValueError:
        mpf.plot(df2,
                 type='candle',
                 ax=ax2,
                 style=s,
                 axtitle=title_2 + f' Bins: {len(volprofile2)}',
                 hlines=dict(hlines=sr2 + p2, linestyle='-',
                             colors=c2, linewidths=0.01),
                 xrotation=5)
    ax2.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df2) / 5)))
    for price in sr2:
        ax2.text(0, price, f'{price}', fontsize=12, bbox=dict(facecolor='blue', alpha=0.25),
                 horizontalalignment='right', verticalalignment='center')
    for pr in p2:
        ax2.text(len(df2), pr, f'{pr}', fontsize=12, bbox=dict(facecolor='k', alpha=0.25),
                 horizontalalignment='left', verticalalignment='center')

    vpax2 = fig.add_axes(ax2.get_position())
    vpax2.set_axis_off()
    vpax2.set_xlim(right=1.2 * max(volprofile2.values))
    vpax2.barh(volprofile2.keys().values, volprofile2.values, height=bucket_size2, align='center',
               color=vp_color, alpha=0.45)

    bucket_size3 = ((max(df3['high']) - min(df3['high'])) / np.ceil(np.sqrt(len(df3)))) * 0.25
    volprofile3 = df3['volume'].groupby(df3['high'].apply(lambda x: bucket_size3 * round(x / bucket_size3, 0))).sum()
    try:
        mpf.plot(df3,
                 type='candle',
                 ax=ax3,
                 style=s,
                 axtitle=title_3 + f' Bins: {len(volprofile3)}',
                 hlines=dict(hlines=sr3 + p3, linestyle='-',
                             colors=c3, linewidths=0.01),

                 vlines=dict(vlines=datetime.datetime.now() - datetime.timedelta(days=3), linestyle='-.', colors='brown'),
                 xrotation=5)
    except ValueError:
        mpf.plot(df3,
                 type='candle',
                 ax=ax3,
                 style=s,
                 axtitle=title_3 + f' Bins: {len(volprofile3)}',
                 hlines=dict(hlines=sr3 + p3, linestyle='-',
                             colors=c3, linewidths=0.01),
                 xrotation=5)

    ax3.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df3) / 5)))
    for price in sr3:
        ax3.text(0, price, f'{price}', fontsize=12, bbox=dict(facecolor='blue', alpha=0.25),
                 horizontalalignment='right', verticalalignment='center')
    for pr in p3:
        ax3.text(len(df3), pr, f'{pr}', fontsize=12, bbox=dict(facecolor='k', alpha=0.25),
                 horizontalalignment='left', verticalalignment='center')

    vpax3 = fig.add_axes(ax3.get_position())
    vpax3.set_axis_off()
    vpax3.set_xlim(right=1.2 * max(volprofile3.values))
    vpax3.barh(volprofile3.keys().values, volprofile3.values, height=bucket_size3, align='center',
               color=vp_color, alpha=0.45)

    bucket_size4 = ((max(df4['high']) - min(df4['high'])) / np.ceil(np.sqrt(len(df4)))) * 0.25
    volprofile4 = df4['volume'].groupby(df4['high'].apply(lambda x: bucket_size4 * round(x / bucket_size4, 0))).sum()

    mpf.plot(df4,
             type='candle',
             ax=ax4,
             style=s,
             axtitle=title_4 + f' Bins: {len(volprofile4)}',
             hlines=dict(hlines=sr4 + p4, linestyle='-',
                         colors=c4, linewidths=0.01),
             xrotation=5)
    ax4.xaxis.set_major_locator(plticker.MultipleLocator(base=int(len(df4) / 5)))
    for price in sr4:
        ax4.text(0, price, f'{price}', fontsize=12, bbox=dict(facecolor='blue', alpha=0.25),
                 horizontalalignment='right', verticalalignment='center')
    for pr in p4:
        ax4.text(len(df4), pr, f'{pr}', fontsize=12, bbox=dict(facecolor='k', alpha=0.25),
                 horizontalalignment='left', verticalalignment='center')
    vpax4 = fig.add_axes(ax4.get_position())
    vpax4.set_axis_off()
    vpax4.set_xlim(right=1.2 * max(volprofile4.values))
    vpax4.barh(volprofile4.keys().values, volprofile4.values, height=bucket_size4, align='center',
               color=vp_color, alpha=0.45)
    fig.show()
    if return_fig:
        return fig
    return


def get_market_data_for_symbols(symbols, turnover_threshold, ignore_already_plotted):
    df = executions.binance_futures_markets().apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1)
    if turnover_threshold is not None:
        df = df[df['turnover'] > dp.string_number_to_integer(turnover_threshold)]
    if symbols is None:
        watchlist_symbols_df = executions.symbols_in_play()
        if ignore_already_plotted:
            watchlist_symbols_df = watchlist_symbols_df[watchlist_symbols_df['plotted'] == False]
        symbols = list(watchlist_symbols_df.ticker)
    df = df[df['ticker'].isin(symbols)].reset_index(drop=True)
    return df


def selected_symbols(symbols, turnover_threshold):
    t = datetime.datetime.now()
    df = get_market_data_for_symbols(symbols, turnover_threshold, False)
    intervals = enumeration.binance_futures_watchlist_intervals()
    for index, row in df.iterrows():
        candles = dp.binance_futures_candles(row['ticker'], intervals)
        plot_symbols(candles, row, True)
        plt.show()
    print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Selected symbols. Created in: '
          f'{datetime.datetime.now() - t}.')
    return


def watchlist_symbols(ignore_already_plotted, turnover_threshold):
    t = datetime.datetime.now()
    df = get_market_data_for_symbols(None, turnover_threshold, ignore_already_plotted)
    intervals = enumeration.binance_futures_watchlist_intervals()
    for index, row in df.iterrows():
        candles = dp.binance_futures_candles(row['ticker'], intervals)
        plot_symbols(candles, row, True)
        plt.show()
        modifications.update_db(queries.update_watchlist_table('BINANCE_FUTURES', row['ticker']))
    print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Watchlist symbols. Created in: '
          f'{datetime.datetime.now() - t}.')
    return


if __name__ == '__main__':
    #selected_symbols(['BTCUSDT', 'ETHUSDT', 'SOLUSDT'], turnover_threshold='30 000 000')
    watchlist_symbols(ignore_already_plotted=True, turnover_threshold='30 000 000')
