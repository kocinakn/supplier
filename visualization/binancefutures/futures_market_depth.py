from tools import data_preparation as dp
from tools import connections
import matplotlib.pyplot as plt
import pandas as pd
import matplotlib.ticker as ticker_matplotlib
import seaborn as sns
import datetime
import warnings
warnings.filterwarnings("ignore")
pd.options.display.float_format = '{:20,.5f}'.format
plt.style.use('ggplot')

pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', None)


def prepare_market_depth_figure_to_plot(ticker, param, mark_price, df_with_threshold, threshold, blocks):
    fig, ax = plt.subplots(figsize=[15, 12])

    sns.ecdfplot(x="price", weights="usd_depth", stat="count",
                 complementary=True, data=df_with_threshold.query("side == 'bids'"),
                 color="green", ax=ax)
    sns.ecdfplot(x="price", weights="usd_depth", stat="count",
                 data=df_with_threshold.query("side == 'asks'"), color="red",
                 ax=ax)

    ax.set_title(
        f"{ticker}PERP Order Book - Depth Chart. Given depth: {threshold}. Depth chosen: {round(param * 100, 2)}%.")
    ax.set_xlabel("Price")
    ax.set_ylabel("USD")

    tick_spacing_x = (ax.get_xticks()[-1] - ax.get_xticks()[0]) / 25
    tick_spacing_y = (ax.get_yticks()[-1] - ax.get_yticks()[0]) / 25
    ax.xaxis.set_major_locator(ticker_matplotlib.MultipleLocator(tick_spacing_x))
    ax.yaxis.set_major_locator(ticker_matplotlib.MultipleLocator(tick_spacing_y))
    ax.yaxis.grid(color='gray', linestyle='dashed')
    ax.xaxis.grid(color='gray', linestyle='dashed')
    ax.set_zorder(3)

    plt.vlines(x=mark_price, ymin=0, ymax=dp.get_ymax_for_depth(df_with_threshold), colors='k')

    ax.text(mark_price, dp.get_ymax_for_depth(df_with_threshold), f'{round(mark_price, 3)}', fontsize=12,
            bbox=dict(facecolor='blue', alpha=0.5),
            horizontalalignment='right', verticalalignment='center')
    ax.get_yaxis().set_major_formatter(ticker_matplotlib.FuncFormatter(lambda x, p: format(int(x), ',')))
    for pr in list(blocks.price):
        plt.vlines(x=pr, ymin=0, ymax=dp.get_ymax_for_depth(df_with_threshold), colors='b', linewidths=0.5)
    return fig


def market_depth(ticker, threshold, return_fig=False):
    client = connections.connect_binance_futures_api()
    print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
          f'Threshold for {ticker} market depth: {threshold} +/- from mark_price.')

    df = dp.download_binance_futures_orderbook_depth(client, ticker)
    ticker_price = dp.download_binance_futures_ticker_price(client, ticker)

    mark_price = float(ticker_price['markPrice'])

    param = dp.check_for_most_narrow_param(df, threshold, mark_price)

    df_with_threshold = df[(df['price'] < (1 + param) * mark_price) & (df['price'] > (1 - param) * mark_price)]

    blocks = df_with_threshold.sort_values('usd_depth', ascending=False).reset_index(drop=True).head(5)

    print(blocks.sort_values('price').reset_index(drop=True))

    fig = prepare_market_depth_figure_to_plot(ticker, param, mark_price, df_with_threshold, threshold, blocks)

    if return_fig is True:
        return fig
    plt.show()
    return df


if __name__ == '__main__':
    ticks = ['FXSUSDT']
    for t in ticks:
        depth_df = market_depth(t, '20%')
