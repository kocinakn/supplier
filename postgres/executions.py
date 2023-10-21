from tools import connections
from postgres import queries
import pandas as pd


def active_coingecko_listed_projects():
    return pd.read_sql_query(queries.coingecko_unbanned(), con=connections.connect_postgres())


def binance_futures_markets():
    return pd.read_sql_query(queries.binance_futures_market(), con=connections.connect_postgres())


def binance_futures_stats():
    return pd.read_sql_query(queries.binance_futures_stats(), con=connections.connect_postgres())


def binance_spot_market():
    return pd.read_sql_query(queries.binance_spot_market(), con=connections.connect_postgres())


def binance_spot_stats():
    return pd.read_sql_query(queries.binance_spot_stats(), con=connections.connect_postgres())


def blacklisted_projects():
    return pd.read_sql_query(queries.blacklisted_projects(), con=connections.connect_postgres())


def coingecko_listed_projects():
    return pd.read_sql_query(queries.coingecko(), con=connections.connect_postgres())


def defi_dapps_market():
    return pd.read_sql_query(queries.defi_dapps_market(), con=connections.connect_postgres())


def defi_dapps_stats():
    return pd.read_sql_query(queries.defi_dapps_stats(), con=connections.connect_postgres())


def defi_on_given_chain():
    return pd.read_sql_query(queries.defi_on_given_chain(), con=connections.connect_postgres())


def kucoin_spot_market():
    return pd.read_sql_query(queries.kucoin_spot_market(), con=connections.connect_postgres())


def on_radar():
    return pd.read_sql_query(queries.on_radar(), con=connections.connect_postgres())


def stablecoins_market():
    return pd.read_sql_query(queries.stablecoins_market(), con=connections.connect_postgres())


def symbols_in_play():
    return pd.read_sql_query(queries.symbols_in_play(), con=connections.connect_postgres())


def symbols_in_play_by_default():
    return pd.read_sql_query(queries.symbols_in_play_by_default(), con=connections.connect_postgres())


def tvl_per_chain_market():
    return pd.read_sql_query(queries.tvl_per_chain_market(), con=connections.connect_postgres())


def wallets_tracker_threshold():
    return pd.read_sql_query(queries.wallets_tracker_threshold(), con=connections.connect_postgres())


def wallets_tracked_addresses():
    return pd.read_sql_query(queries.wallets_tracked_addresses(), con=connections.connect_postgres())


def wallets_tracked_responses():
    return pd.read_sql_query(queries.wallets_tracked_responses(), con=connections.connect_postgres())


def wallets_activities():
    return pd.read_sql_query(queries.wallets_activities(), con=connections.connect_postgres())


def twitter_accounts():
    return pd.read_sql_query(queries.twitter_accounts(), con=connections.connect_postgres())


def twitter_accounts_to_check():
    return pd.read_sql_query(queries.twitter_accounts_to_check(), con=connections.connect_postgres())


