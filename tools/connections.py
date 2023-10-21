from binance.client import Client as BinanceSpotClient
from binance.futures import Futures as BinanceFuturesClient
from pybithumb import Bithumb
from pycoingecko import CoinGeckoAPI
from kucoin.client import Client as KucoinSpotClient

from tools import credentials
import requests

import tweepy
import gate_api
import psycopg2


def connect_binance_spot_api():
    w = credentials.binance_spot_api()
    return BinanceSpotClient(w[0], w[1])


def connect_binance_futures_api():
    w = credentials.binance_futures_api()
    return BinanceFuturesClient(w[0], w[1])


def connect_bithumb_spot_api():
    return Bithumb


def connect_coingecko_api():
    return CoinGeckoAPI()


def connect_kucoin_api():
    w = credentials.kucoin_spot_api()
    return KucoinSpotClient(w[0], w[1], w[2])


def connect_gateio_api():
    connection = gate_api.Configuration(host="https://api.gateio.ws/api/v4")
    client = gate_api.ApiClient(connection)
    gate = gate_api.SpotApi(client)
    return gate


def connect_postgres():
    session = psycopg2.connect(dbname='postgres', user=credentials.postgres_credentials()[0],
                               password=credentials.postgres_credentials()[1])
    return session


def connect_twitter_api():
    client = tweepy.Client(bearer_token=credentials.twitter_bearer_token(),
                           consumer_key=credentials.twitter_api_key(),
                           consumer_secret=credentials.twitter_api_key_secret(),
                           access_token=credentials.twitter_access_token(),
                           access_token_secret=credentials.twitter_access_token_secret(),
                           return_type=requests.Response,
                           wait_on_rate_limit=True)
    return client
