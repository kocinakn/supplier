import datetime


def coingecko_unbanned():
    q = "SELECT * FROM coingecko where banned is false"
    return q


def binance_futures_market():
    q = "SELECT * FROM binance_futures_market;"
    return q


def binance_futures_stats():
    q = "SELECT * FROM binance_futures_stats;"
    return q


def binance_spot_market():
    q = "SELECT * FROM binance_spot_market;"
    return q


def binance_spot_stats():
    q = "SELECT * FROM binance_spot_stats;"
    return q


def blacklisted_projects():
    q = "SELECT * FROM coingecko where banned is True;"
    return q


def coingecko():
    q = "SELECT * FROM coingecko"
    return q


def defi_dapps_market():
    q = "SELECT * FROM defi_dapps_market;"
    return q


def defi_dapps_stats():
    q = "SELECT * FROM defi_dapps_stats;"
    return q


def defi_on_given_chain():
    q = "SELECT * FROM defi_on_given_chain;"
    return q


def delete_previous_watchlist_tickers(exchange):
    return "DELETE FROM symbols_in_play WHERE exchange = '{}' and date < '{}'".format(exchange, datetime.datetime.now())


def kucoin_spot_market():
    q = "SELECT * FROM kucoin_spot_market;"
    return q


def on_radar():
    q = "SELECT * FROM on_radar"
    return q


def stablecoins_market():
    q = "SELECT * FROM stablecoins_market;"
    return q


def symbols_in_play():
    q = "SELECT * FROM symbols_in_play order by turnover desc;"
    return q


def symbols_in_play_by_default():
    q = "SELECT * FROM symbols_in_play_by_default;"
    return q


def tvl_per_chain_market():
    q = "SELECT * FROM tvl_per_chain_market;"
    return q


def twitter_accounts():
    q = "SELECT * FROM twitter_accounts;"
    return q


def twitter_accounts_to_check():
    return """ SELECT screen_name FROM twitter_accounts"""


def update_binance_futures_markets(ticker):
    return """ UPDATE binance_futures_market
               SET plotted = true
               WHERE ticker = '{}'""".format(ticker)


def update_watchlist_table(exchange, ticker):
    return """ UPDATE symbols_in_play
               SET plotted = true
               WHERE exchange = '{}' and ticker = '{}'""".format(exchange, ticker)


def update_wallets_tracker_threshold(t):
    return """ UPDATE wallets_time_threshold
               SET threshold = '{}' """.format(t)


def wallets_tracker_threshold():
    return """ SELECT threshold FROM wallets_time_threshold """


def wallets_tracked_addresses():
    return """ SELECT * FROM wallets_tracked_addresses order by id """


def wallets_tracked_responses():
    return """ SELECT * FROM wallets_responses_dictionary"""


def wallets_activities():
    return """ SELECT * FROM wallets_activities """

