from postgres import executions
from tools import agents
from tools import enumeration
from tools import connections
from tools import credentials
import webbrowser
import tweepy
import time
import numpy as np

import pandas as pd
import datetime
import requests
import MetaTrader5 as mt5
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', None)


def binance_futures_order_side(price_range):
    if price_range[0] < price_range[1]:
        return 'SELL'
    else:
        return 'BUY'


def download_binance_futures_candlesticks(client, ticker, days, candle_interval):
    limit = amount_of_candles_per_interval(days, candle_interval)
    data = client.continuous_klines(pair=ticker, contractType='PERPETUAL', interval=candle_interval, limit=limit)
    return data


def download_binance_spot_candlesticks(client, ticker, candle_interval, days):
    days = int(days[:-1])
    start_timestamp = int((datetime.datetime.now() - datetime.timedelta(days=int(days))).timestamp())
    end_timestamp = int(datetime.datetime.now().timestamp())
    return client.get_historical_klines(ticker, candle_interval, start_timestamp * 1000, end_timestamp * 1000)


def download_kucoin_spot_candlesticks(client, ticker, candle_interval, days):
    days = int(days[:-1])
    candle_interval = fit_candle_interval_for_kucoin_api(candle_interval)
    start_timestamp = int((datetime.datetime.now() - datetime.timedelta(days=int(days))).timestamp())
    end_timestamp = int(datetime.datetime.now().timestamp())
    return client.get_kline_data(ticker, candle_interval, start_timestamp, end_timestamp)


def download_gateio_spot_candlesticks(client, ticker, candle_interval, days):
    days = int(days[:-1])
    start_timestamp = int((datetime.datetime.now() - datetime.timedelta(days=int(days))).timestamp())
    end_timestamp = int(datetime.datetime.now().timestamp())
    return client.list_candlesticks(ticker, _from=start_timestamp, to=end_timestamp, interval=candle_interval)


def download_huobi_spot_candlesticks(ticker, candle_interval, days):
    days = int(days[:-1])
    if type(candle_interval) != list:
        candle_interval = fit_candle_interval_for_huobi_api(days, candle_interval)
    try:
        curl = f'https://api.huobi.pro/market/history/kline?period={candle_interval[0]}&size={candle_interval[1]}&symbol={ticker}'
        resp = get_json(curl, True)
        df = pd.DataFrame(resp['data'])
    except KeyError:
        cdl = input(f'Candle interval invalid: {candle_interval}. Write in valid parameter.')
        candle_interval = fit_candle_interval_for_huobi_api(days, cdl)
        curl = f'https://api.huobi.pro/market/history/kline?period={candle_interval[0]}&size={candle_interval[1]}&symbol={ticker}'
        resp = get_json(curl, True)
        df = pd.DataFrame(resp['data'])
    return df


def download_bithumb_candlesticks(client, ticker, candle_interval):
    return client.get_candlestick(ticker.replace('KRW', ''), payment_currency="KRW", chart_intervals=f'{candle_interval}')


def download_mexc_spot_candlesticks(ticker, candle_interval, days):
    days = int(days[:-1])
    current_time = datetime.datetime.now()
    timedelta = int((current_time - datetime.timedelta(days=int(days))).timestamp())
    curl = f'https://www.mexc.com/open/api/v2/market/kline?' \
           f'symbol={ticker}&interval={candle_interval}&start_time={timedelta}&limit=1000'
    response = get_json(curl, True)
    return response


def download_coingecko_candlesticks(coin, interval_in_days):
    if 'd' in interval_in_days:
        interval_in_days = int(interval_in_days[:-1])
    api_id = get_coingecko_api_id_for_coin(coin)
    cg_req = get_json(f'https://api.coingecko.com/api/v3/coins/{api_id}/ohlc?vs_currency=usd&days={interval_in_days}', True)
    return pd.DataFrame(cg_req)


def download_mt5_candlesticks(ticker, days, candle_interval):
    if not mt5.initialize():
        print("initialize() failed")
        mt5.shutdown()
    mt5_candle_type = enumeration.get_candle_type_for_mt5(candle_interval)
    candles_amount = amount_of_candles_per_interval(days, candle_interval, False)
    return mt5.copy_rates_from(ticker, mt5_candle_type, datetime.datetime.now(), candles_amount)


def download_coingecko_spot_markets_table(api_id):
    return get_json(f'https://api.coingecko.com/api/v3/coins/{api_id}/tickers?order=volume_desc&depth=true', True)


def download_binance_futures_orderbook_depth(client, ticker):
    ob_depth = client.depth(symbol=ticker, limit=1000)
    return prepare_depth_dataframe(ob_depth)


def download_binance_futures_ticker_price(client, symbol):
    return client.mark_price(symbol)


def download_binance_futures_24h_stats(client):
    return pd.DataFrame(client.ticker_24hr_price_change())


def download_binance_futures_open_interest(client, ticker, days, candle_interval):
    amount = amount_of_candles_per_interval(days, candle_interval, False, True)
    return client.open_interest_hist(symbol=ticker, period=candle_interval, limit=amount)


def download_binance_futures_symbols(client):
    return pd.DataFrame(client.exchange_info()['symbols'])


def download_binance_spot_symbols(client):
    return pd.DataFrame(client.get_exchange_info()['symbols'])


def download_binance_spot_24h_stats(client):
    return pd.DataFrame(client.get_ticker())


def download_binance_spot_orderbook_depth(client, ticker):
    ob_depth = client.get_order_book(symbol=ticker, limit=1000)
    return prepare_depth_dataframe(ob_depth)


def download_kucoin_spot_symbols():
    return pd.DataFrame(get_json('https://openapi-v2.kucoin.com/api/v1/market/allTickers', True)['data']['ticker'])


def download_coingecko_market_info_for_api_id(api_id):
    while True:
        try:
            url = f'https://api.coingecko.com/api/v3/coins/{api_id}?localization=false&tickers=false&market_data=true'
            return get_json(url, True)
        except Exception:
            continue


def download_defillama_protocols():
    return pd.DataFrame(get_json('https://api.llama.fi/protocols', True))


def download_defillama_chains():
    return pd.DataFrame(get_json('https://api.llama.fi/chains', True))


def download_defillama_stablecoins():
    data = get_json('https://stablecoins.llama.fi/stablecoins?includePrices=true', True)
    return pd.DataFrame(data['peggedAssets']).sort_values('gecko_id')


def binance_spot_candles(ticker, candlestick_intervals=None):
    candles = {}
    client = connections.connect_binance_spot_api()
    if candlestick_intervals is None:
        candlestick_intervals = enumeration.binance_spot_candlesticks_intervals(client)

    for days, candle_interval in candlestick_intervals.items():
        ohlc = download_binance_spot_candlesticks(client, ticker, candle_interval, days)
        candles[days] = prepared_binance_spot_ohlc(ohlc)
        time.sleep(2)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker}, intervals: '
          f'{candlestick_intervals}. Source: BINANCE')
    return candles


def prepared_binance_spot_ohlc(ohlc):
    df = pd.DataFrame(ohlc).iloc[:, 0:6].apply(lambda x: x.apply(lambda y: ret_float(y)), axis=1)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    df['timestamp'] = [int(i / 1000) for i in df.timestamp]
    df = prepared_ohlc(df)  #binance_spot
    return df


def kucoin_spot_candles(ticker, candlesticks_intervals=None):
    candles = {}
    client = connections.connect_kucoin_api()
    if candlesticks_intervals is None:
        candlesticks_intervals = enumeration.kucoin_spot_candlesticks_intervals()
    ticker = ticker[:-4] + '-' + ticker[-4:]

    for days, candle_interval in candlesticks_intervals.items():
        ohlc = download_kucoin_spot_candlesticks(client, ticker, candle_interval, days)
        candles[days] = prepared_kucoin_spot_ohlc(ohlc)
        time.sleep(12)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker}, intervals: '
          f'{candlesticks_intervals}. Source: KUCOIN')
    return candles


def prepared_kucoin_spot_ohlc(ohlc):
    df = pd.DataFrame(ohlc).iloc[:, :6].apply(lambda x: x.apply(lambda y: ret_float(y)), axis=1)
    df.columns = ['timestamp', 'open', 'close', 'high', 'low', 'volume']
    df = prepared_ohlc(df)  #kucoin_spot
    return df


def gateio_spot_candles(ticker, candlesticks_intervals=None):
    candles = {}
    client = connections.connect_gateio_api()
    if candlesticks_intervals is None:
        candlesticks_intervals = enumeration.gateio_spot_candlesticks_intervals()

    for days, candle_interval in candlesticks_intervals.items():
        ohlc = download_gateio_spot_candlesticks(client, ticker, candle_interval, days)
        candles[days] = prepared_gateio_spot_ohlc(ohlc)
        time.sleep(2)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker}, intervals: '
          f'{candlesticks_intervals}. Source: GATEIO')
    return candles


def prepared_gateio_spot_ohlc(ohlc):
    df = pd.DataFrame(ohlc).apply(lambda x: x.apply(lambda y: ret_float(y)), axis=1)
    df.columns = ['timestamp', 'turnover', 'open', 'high', 'low', 'close', 'volume']
    df = prepared_ohlc(df)  #gateio_spot
    return df


def binance_futures_candles(ticker, candlestick_intervals=None):
    candles = {}
    client = connections.connect_binance_futures_api()
    if candlestick_intervals is None:
        candlestick_intervals = enumeration.binance_futures_candlesticks_intervals()

    for days, candle_features in candlestick_intervals.items():
        ohlc = download_binance_futures_candlesticks(client, ticker, days, candle_features)
        candles[days] = prepared_binance_futures_ohlc(ohlc)
        time.sleep(2)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker}, intervals: '
          f'{candlestick_intervals}. Source: BINANCE_FUTURES')

    return candles


def prepared_binance_futures_ohlc(ohlc):
    df = pd.DataFrame(ohlc)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                  'turnover', 'trades', 'taker_vol', 'taker_turnover', 'ignore']
    df = df.sort_values('timestamp', ascending=True)
    df = df.applymap(convert_to_float)
    df['timestamp'] = [int(i / 1000) for i in df.timestamp]
    df = prepared_ohlc(df)
    return df


def huobi_spot_candles(ticker, candlestick_intervals=None):
    candles = {}
    if candlestick_intervals is None:
        candlestick_intervals = enumeration.huobi_spot_candlesticks_intervals()
    ticker = ticker.lower()

    for days, candle_interval in candlestick_intervals.items():
        ohlc = download_huobi_spot_candlesticks(ticker, candle_interval, days)
        candles[days] = prepared_huobi_spot_ohlc(ohlc)
        time.sleep(2)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker.upper()}, intervals: '
          f'{candlestick_intervals}. Source: HUOBI')
    return candles


def prepared_huobi_spot_ohlc(df):
    df = df.rename(columns={"id": "timestamp", "amount": "volume"})
    df = df.apply(lambda x: x.apply(lambda y: ret_float(y)), axis=1)
    df = prepared_ohlc(df)  # huobi_spot
    return df


def bithumb_spot_candles(ticker, candlestick_intervals=None):
    candles = {}
    client = connections.connect_bithumb_spot_api()
    if candlestick_intervals is None:
        candlestick_intervals = enumeration.bithumb_candlesticks_intervals()

    for days, candle_interval in candlestick_intervals.items():
        ohlc = download_bithumb_candlesticks(client, ticker, candle_interval)
        candles[days] = prepared_bithumb_ohlc(ohlc, days)
        time.sleep(2)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker}, intervals: '
          f'{candlestick_intervals}. Source: Bithumb')
    return candles


def prepared_bithumb_ohlc(ohlc, days):
    days = int(days[:-1])
    current_time = datetime.datetime.now()
    timedelta = (current_time - datetime.timedelta(days=int(days)))
    df = pd.DataFrame(ohlc).apply(lambda x: x.apply(lambda y: ret_float(y)), axis=1)
    df = df[df.index > timedelta]
    df[['open', 'high', 'low', 'close']] = df[['open', 'high', 'low', 'close']] / usdkrw_cross()
    df['timestamp'] = [int(datetime.datetime.timestamp(date)) for date in df.index]
    df = prepared_ohlc(df)  # bithumb_spot
    return df


def mexc_spot_candles(ticker, candlestick_intervals=None):
    candles = {}
    if candlestick_intervals is None:
        candlestick_intervals = enumeration.mexc_spot_candlesticks_intervals()
    ticker = ticker.replace('USDT', '') + '_' + 'USDT'
    for days, candle_interval in candlestick_intervals.items():
        ohlc = download_mexc_spot_candlesticks(ticker, candle_interval, days)
        candles[days] = prepared_mexc_spot_ohlc(ohlc)
        time.sleep(2)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker}, intervals: '
          f'{candlestick_intervals}. Source: MEXC')
    return candles


def prepared_mexc_spot_ohlc(response):
    df = pd.DataFrame(response['data'])
    df = df.apply(lambda x: x.apply(lambda y: ret_float(y)), axis=1)
    df.columns = ['timestamp', 'open', 'close', 'high', 'low', 'volume', 'turnover']
    return prepared_ohlc(df) # mexc_spot


def coingecko_spot_candles(coin):
    candles = {}
    candlestick_intervals = enumeration.coingecko_spot_candlesticks_intervals()
    for days, candle_interval in candlestick_intervals.items():
        ohlc = download_coingecko_candlesticks(coin, interval_in_days=days)
        days = switch_coingecko_max_interval(days)
        candles[days] = prepared_coingecko_spot_ohlc(ohlc)
    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {coin}USDT, intervals: '
          f'{candlestick_intervals}. Source: CG')
    return candles


def prepared_coingecko_spot_ohlc(ohlc):
    df = pd.DataFrame(ohlc)
    df.columns = ['timestamp', 'open', 'high', 'low', 'close']
    df['volume'] = 0
    df['timestamp'] = [int(i / 1000) for i in df.timestamp]
    df = prepared_ohlc(df) # coingecko_spot
    return df


def mt5_candles(ticker, candlestick_intervals=None):
    candles = {}
    if candlestick_intervals is None:
        candlestick_intervals = enumeration.intervals_for_mt5_tickers()

    for days, candle_interval in candlestick_intervals.items():
        ohlc = download_mt5_candlesticks(ticker, days, candle_interval)
        candles[days] = prepared_mt5_ohlc(ohlc)

    print(f'{datetime_desired_format(datetime.datetime.now())}. Received candlesticks for {ticker}, intervals: '
          f'{candlestick_intervals}. Source: META_TRADER5')
    return candles


def prepared_mt5_ohlc(ohlc):
    df = pd.DataFrame(ohlc)
    df.rename(columns={'tick_volume': 'volume', 'time': 'timestamp'}, inplace=True)
    df = prepared_ohlc(df) # mt5
    return df


def prepared_ohlc(df):
    d = df.sort_values('timestamp', ascending=True)
    d.index = [datetime.datetime.fromtimestamp(int(i)) for i in d.timestamp]
    d['date_string'] = [date.strftime('%Y-%m-%d %H:%M:%S') for date in d.index]
    return d[['open', 'high', 'low', 'close', 'volume', 'timestamp', 'date_string']]


def spot_candles_for_coin(coin, intervals):
    best_sources = spot_markets_table_for_coin(coin)
    candles = determine_source_exchange_and_get_spot_candles_for_ticker(best_sources, coin, intervals)
    return candles


def spot_markets_table_for_coin(coin, limit: int = 5):
    projects, api_id = get_coingecko_api_id_for_coin(coin, True)
    data = download_coingecko_spot_markets_table(api_id)
    table = None
    for row_data in data['tickers']:
        table = coingecko_spot_markets_table(table, projects, row_data)
        if table is not None:
            if len(table) == limit:
                break
    table.columns = ['exchange', 'pair', 'price', 'spread', 'plus_2%', 'minus_2%', 'volume', 'updated', 'status']
    return table.reset_index(drop=True)


def coingecko_spot_markets_table(table, projects, row_data):
    try:
        exchange_name = row_data['market']['name']
        base_currency = projects[projects.api_id == row_data['coin_id']].symbol.iloc[0]
        try:
            quote_currency = projects[projects.api_id == row_data['target_coin_id']].symbol.iloc[0]
        except IndexError:
            quote_currency = 'USDT'
        pair = f'{base_currency}{quote_currency}'
        price = row_data['converted_last']['usd']
        volume = string_number_from_integer(round(row_data['converted_volume']['usd']))
        spread = row_data['bid_ask_spread_percentage']
        depthup, depthdown = row_data['cost_to_move_up_usd'], row_data['cost_to_move_down_usd']
        updated, status = row_data['last_fetch_at'], row_data['trust_score']
        row = [exchange_name, pair, price, spread, depthup, depthdown, volume, updated, status]
        if status == 'red':
            return
        if table is None:
            table = pd.DataFrame(row).T
        else:
            table = pd.concat([table, pd.DataFrame(row).T])
    except KeyError:
        return table
    except IndexError:
        return table
    return table


def determine_source_exchange_and_get_spot_candles_for_ticker(df, coin, intervals):
    print(df)
    instr = input(f'{datetime_desired_format(datetime.datetime.now())}. Type in instrument for {coin} candlesticks '
                  f'e.g. BINANCE_{coin}USDT, CG_{coin}: ')
    source_ex, ticker = instr.rsplit('_')[0], instr.rsplit('_')[1]
    candles = candlesticks_from_determined_source_exchange(source_ex, ticker, intervals)
    return candles


def candlesticks_from_determined_source_exchange(source_ex, ticker, intervals):
    if source_ex == 'KUCOIN':
        return kucoin_spot_candles(ticker, intervals)
    elif source_ex == 'BINANCE':
        return binance_spot_candles(ticker, intervals)
    elif source_ex == 'GATEIO':
        return gateio_spot_candles(ticker, intervals)
    elif source_ex == 'HUOBI':
        return huobi_spot_candles(ticker, intervals)
    elif source_ex == 'MEXC':
        return mexc_spot_candles(ticker, intervals)
    elif source_ex == 'BITHUMB':
        return bithumb_spot_candles(ticker, intervals)
    elif source_ex == 'CG':
        return coingecko_spot_candles(ticker)
    else:
        return None


def prepare_depth_dataframe(depth):
    df_list = []
    for side in ["bids", "asks"]:
        df = pd.DataFrame(depth[side], columns=["price", "quantity"], dtype=float)
        df["side"] = side
        df_list.append(df)
    df = pd.concat(df_list).reset_index(drop=True)
    df['usd_depth'] = df.price * df.quantity
    return df


def check_for_most_narrow_param(df, threshold, mark_price):
    manual_depth = float(int(threshold[:-1]) / 100)
    bid_lowest, ask_highest = df.nsmallest(1, 'price').price.iloc[0], df.nlargest(1, 'price').price.iloc[0]
    real_bid_depth = abs(round((mark_price - bid_lowest) / mark_price, 3))
    real_ask_depth = abs(round((ask_highest - mark_price) / ask_highest, 3))
    candidates = [manual_depth, real_bid_depth, real_ask_depth]
    most_narrow_depth = min(candidates)
    print(f'{datetime_desired_format(datetime.datetime.now())}. Depth given: {threshold}. Real bid_depth: '
          f'{round((real_bid_depth * 100), 2)}%, Real ask_depth: {round((real_ask_depth * 100), 2)}%. '
          f'Depth chosen: {round((most_narrow_depth * 100), 2)}%')
    return most_narrow_depth


def get_ymax_for_depth(df):
    sum_bid = df[df.side == 'bids'].usd_depth.sum()
    sum_ask = df[df.side == 'asks'].usd_depth.sum()
    if sum_ask > sum_bid:
        return int(round(sum_ask))
    else:
        return int(round(sum_bid))


def binance_futures_open_interest(ticker, oi_intervals, client=None):
    d = {}
    if client is None:
        client = connections.connect_binance_futures_api()
    for days, candle_interval in oi_intervals.items():
        oi = download_binance_futures_open_interest(client, ticker, days, candle_interval)
        d[days] = prepared_binance_futures_open_interest(oi)
        time.sleep(2)
    print(f'{datetime_desired_format(datetime.datetime.now())}. Received open_interest for {ticker}, intervals: '
          f'{oi_intervals}. Source: BINANCE_FUTURES')
    return d


def prepared_binance_futures_open_interest(oi_data):
    df = pd.DataFrame(oi_data)
    df['sumOpenInterest'] = df['sumOpenInterest'].astype('float').astype('int64')
    df['sumOpenInterestValue'] = df['sumOpenInterestValue'].astype('float').astype('int64')
    df['timestamp'] = [int(i / 1000) for i in df.timestamp]
    df = df.sort_values('timestamp', ascending=True)
    df.index = [datetime.datetime.fromtimestamp(int(i)) for i in df.timestamp]
    df['date_string'] = [date.strftime('%Y-%m-%d %H:%M:%S') for date in df.index]
    return df


def binance_futures_symbols(client, main_quote_currency, secondary_quote_currency):
    df = download_binance_futures_symbols(client)
    symbols = filter_inactive_binance_futures_symbols(df)
    usdt_margined, busd_margined = sort_binance_futures_symbols_by_margin_asset(symbols)
    symbols = filtered_symbols(usdt_margined, busd_margined, main_quote_currency, secondary_quote_currency)
    return symbols


def filter_inactive_binance_futures_symbols(df):
    df = df[df['contractType'] == 'PERPETUAL']
    df = df[df['status'] == 'TRADING']
    return df


def sort_binance_futures_symbols_by_margin_asset(df):
    usdt_margined = []
    busd_margined = []
    for index, row in df.iterrows():
        if row['marginAsset'] == 'USDT':
            usdt_margined.append(row['baseAsset'])
        else:
            busd_margined.append(row['baseAsset'])
    return usdt_margined, busd_margined


def filtered_symbols(usdt_margined, non_usdt_margined, main_quote_currency, secondary_quote_currency):
    symbols = []
    if main_quote_currency == 'USDT':
        symbols = symbols_with_usdt_as_main_quote_currency(symbols, usdt_margined, non_usdt_margined,
                                                           main_quote_currency, secondary_quote_currency)
    else:
        symbols = symbols_with_non_usdt_as_main_quote_currency(symbols, usdt_margined, non_usdt_margined,
                                                               main_quote_currency, secondary_quote_currency)
    return sorted(symbols)


def binance_spot_symbols(client, main_quote_currency, secondary_quote_currency):
    df = download_binance_spot_symbols(client)
    symbols = filter_for_required_binance_spot_symbols(df)
    usdt_quoted, busd_quoted = sort_symbols_by_quote_asset(symbols)
    symbols = filtered_symbols(usdt_quoted, busd_quoted, main_quote_currency, secondary_quote_currency)
    return symbols


def filter_for_required_binance_spot_symbols(df):
    df = df[df['quoteAsset'].str.contains('USDT|BUSD')]
    df = df[~df['baseAsset'].str.contains('BULL|BEAR|UP|DOWN')]
    df = df[df['status'] == 'TRADING']
    print(f"{datetime_desired_format(datetime.datetime.now())}. Symbols filtered: USDT, BUSD, TRADING.")
    return df


def sort_symbols_by_quote_asset(symbols):
    usdt_quoted = []
    non_usdt_quoted = []
    for index, row in symbols.iterrows():
        if row['quoteAsset'] == 'USDT':
            usdt_quoted.append(row['baseAsset'])
        else:
            non_usdt_quoted.append(row['baseAsset'])
    return usdt_quoted, non_usdt_quoted


def symbols_with_usdt_as_main_quote_currency(symbols, usdt_margined, non_usdt_margined,
                                             main_quote_currency, secondary_quote_currency):
    for base_asset in non_usdt_margined:
        if base_asset in usdt_margined:
            continue
        else:
            symbols.append(f'{base_asset}{secondary_quote_currency}')
    for base_asset in usdt_margined:
        symbols.append(f'{base_asset}{main_quote_currency}')
    return symbols


def symbols_with_non_usdt_as_main_quote_currency(symbols, usdt_margined, non_usdt_margined, main_quote_currency, secondary_quote_currency):
    for base_asset in usdt_margined:
        if base_asset in non_usdt_margined:
            continue
        else:
            symbols.append(f'{base_asset}{secondary_quote_currency}')
    for base_asset in non_usdt_margined:
        symbols.append(f'{base_asset}{main_quote_currency}')
    return symbols


def binance_spot_market_table(client):
    df = download_binance_spot_24h_stats(client)
    df = df[['symbol', 'lastPrice', 'quoteVolume', 'count']].apply(lambda x: x.apply(lambda y: ret_float(y)))
    df['date'] = datetime_desired_format(datetime.datetime.now())
    return df


def kucoin_spot_symbols(main_quote_currency, secondary_quote_currency):
    df = download_kucoin_spot_symbols()
    df = filter_for_required_kucoin_spot_symbols(df)
    usdt_quoted, usdc_quoted = sort_symbols_by_quote_asset(df)
    symbols = filtered_symbols(usdt_quoted, usdc_quoted, main_quote_currency, secondary_quote_currency)
    return df[df['symbol'].isin(symbols)].reset_index(drop=True)


def filter_for_required_kucoin_spot_symbols(df):
    df['baseAsset'], df['quoteAsset'] = '', ''
    for index, row in df.iterrows():
        s = row['symbol'].rsplit('-')
        df.loc[index, 'baseAsset'], df.loc[index, 'quoteAsset'] = s[0], s[1]
    df = df[df['quoteAsset'].str.contains('USDT|USDC')]
    df = df[~df['baseAsset'].str.contains('3L|3S|HI')]
    df['symbol'] = df['symbol'].str.replace('-', '')
    print(f"{datetime_desired_format(datetime.datetime.now())}. Kucoin: USDT and USDC pairs included.")
    return df


def defillama_chains():
    df = download_defillama_chains()
    df = prepared_defillama_chains(df)
    return df


def prepared_defillama_chains(df):
    df = df[['name', 'tokenSymbol', 'gecko_id', 'tvl']]
    df['share'] = df['tvl'] / df['tvl'].sum()
    df['date'] = datetime_desired_format(datetime.datetime.now())
    df = df.sort_values('tvl', ascending=False).reset_index(drop=True)
    return df


def defillama_protocols():
    df = download_defillama_protocols()
    df = filter_defillama_protocols(df)
    df = prepared_defillama_protocols(df)
    return df


def filter_defillama_protocols(df):
    blacklisted = executions.blacklisted_projects()
    blacklisted_api_ids = list(blacklisted.api_id)
    df = df[~df['gecko_id'].isin(blacklisted_api_ids)]
    return df


def prepared_defillama_protocols(df):
    fr = df[['name', 'symbol', 'gecko_id', 'category', 'tvl', 'mcap', 'chains', 'chainTvls']]. \
        sort_values('tvl', ascending=False).fillna(0).reset_index(drop=True)
    fr['date'] = datetime_desired_format(datetime.datetime.now())
    return fr


def defillama_stablecoins():
    df = download_defillama_stablecoins()
    df = filter_defillama_stablecoins(df)
    df = prepared_defillama_stablecoins(df)
    return df


def filter_defillama_stablecoins(df):
    accepted_stables = executions.active_coingecko_listed_projects()
    accepted = list(accepted_stables.api_id)
    for index, row in df.iterrows():
        if row['gecko_id'] in accepted:
            peg_type = row['pegType']
            circ = round(row['circulating'][peg_type])
            df.loc[index, 'circulating'] = circ
        else:
            df = df.drop(index)
    return df.reset_index(drop=True)


def prepared_defillama_stablecoins(df):
    df['circulating'] = df['circulating'].astype(float)
    df['date'] = datetime_desired_format(datetime.datetime.now())
    df = df[['symbol', 'name', 'gecko_id', 'price', 'circulating', 'chains', 'date']]
    return df


def ret_float(y):
    try:
        if type(y) == bool:
            return y
        else:
            return float(y)
    except Exception:
        return y


def ret_int(y):
    try:
        if type(y) == bool:
            return y
        else:
            return int(y)
    except Exception:
        return y


def string_number_to_integer(n):
    if n is None:
        param = 0
    else:
        param = int(n.replace(' ', ''))
    return param


def fit_candle_interval_for_kucoin_api(candle_interval):
    if 'm' in candle_interval:
        return candle_interval.replace('m', 'min')
    elif 'h' in candle_interval:
        return candle_interval.replace('h', 'hour')
    elif 'd' in candle_interval:
        return candle_interval.replace('d', 'day')
    else:
        raise TypeError('fit_candle_interval_for_kucoin_api - no assumption for candle interval.')


def fit_candle_interval_for_huobi_api(day, candle_interval):
    days = day
    if 'm' in candle_interval:
        amount = days*24*60/int(candle_interval[:-1])
        return [candle_interval.replace('m', 'min'), int(amount)]
    elif 'hour' in candle_interval:
        amount = days * 24 / int(candle_interval[:-4])
        return [candle_interval, int(amount)]
    elif 'h' in candle_interval:
        amount = days * 24 / int(candle_interval[:-1])
        return [candle_interval.replace('h', 'hour'), int(amount)]
    elif 'd' in candle_interval:
        amount = days / int(candle_interval[:-1])
        return [candle_interval.replace('d', 'day'), int(amount)]
    else:
        raise TypeError('fit_candle_interval_for_huobi_api - no assumption for candle interval.')


def switch_coingecko_max_interval(days):
    if days == 'max':
        return '1825d'
    else:
        return days


def get_coingecko_api_id_for_coin(coin, spot_markets_table=False):
    projects = executions.active_coingecko_listed_projects()
    info = projects[projects.symbol == coin]
    if len(info) == 0:
        projects = executions.coingecko_listed_projects()
        info = projects[projects.symbol == coin]
    if len(info) > 1:
        table = info[['symbol', 'api_id', 'banned', 'description']].reset_index(drop=True)
        print(table)
        cg_id = input(f'{datetime_desired_format(datetime.datetime.now())}. CoinGecko: Input api id')
    else:
        cg_id = info[info.symbol == coin].api_id.iloc[0]
    if spot_markets_table is True:
        return projects, cg_id
    return cg_id


def string_number_from_integer(num):
    number = None
    num_str = str(num)
    param = len(num_str) // 3
    first_numbers_of_new_string = num_str[-len(num_str):-3 * param]
    new_string_objects = []
    for i in reversed(range(param + 1)):
        if i == 0:
            break
        left_bound = -3 * i
        right_bound = -3 * (i - 1)
        if right_bound == 0:
            zzz = num_str[left_bound:]
        else:
            zzz = num_str[left_bound: right_bound]
        new_string_objects.append(f' {zzz}')
    for i in range(len(new_string_objects)):
        if i == 0:
            number = first_numbers_of_new_string + new_string_objects[i]
        if i > 0:
            number = number + new_string_objects[i]
    return number


def usdkrw_cross():
    data = get_json('https://api.korbit.co.kr/v1/ticker/detailed?currency_pair=usdc_krw', True)
    data = pd.DataFrame(data, index=[0]).apply(lambda x: ret_float(x))
    return round((data['last'] + data['open'] + data['bid'] + data['ask'] + data['low'] + data['high']) / 6, 3)


def get_candle_interval(ohlc):
    seconds = int(ohlc.timestamp.iloc[1] - ohlc.timestamp.iloc[0])
    if seconds < 3600:
        return f'{int(seconds/60)}m'
    elif (seconds >= 3600) & (seconds <= 86400):
        return f'{int(seconds/3600)}h'
    else:
        return f'{int(seconds/86400)}d'


def candles_min_and_max_price(ohlc):
    return [ohlc[['close', 'low']].min(axis=1).min(), ohlc[['open', 'high']].max(axis=1).max()]


def fig_title_for_spot_with_one_ticker(ticker, ohlc, resistance_lines, watchlist=False):
    candle_interval = get_candle_interval(ohlc) # for spot and binance_spot
    obj = (pd.to_datetime(ohlc.index[-1]) - pd.to_datetime(ohlc.index[0])).components
    day = obj.days
    hour = obj.hours
    minutes = obj.minutes
    horizontals = len(resistance_lines)
    if watchlist:
        return f"Candle {candle_interval}, start: {ohlc.index[0].date()}, Δ: {day}d, {hour}h, {minutes}m. S/R: {horizontals}."
    return f"{ticker}: Candle {candle_interval}, start: {ohlc.index[0]}, Δ: {day}d, {hour}h, {minutes}m. S/R: {horizontals}."


def fig_title_for_spot_with_spot_reference(base_coin, ref_coin, candles):
    candle_interval = get_candle_interval(candles) # spot reference
    obj = (pd.to_datetime(candles.index[-1]) - pd.to_datetime(candles.index[0])).components
    day = obj.days
    hour = obj.hours
    minutes = obj.minutes
    return f"{base_coin} vs {ref_coin}: candle {candle_interval}, start: {candles.index[0]}, " \
           f"timedelta: {day}d, {hour}h, {minutes}m."


def fig_title_for_mt5_with_one_ticker(ohlc, ticker, resistance_lines, show_weeks):
    candle_interval = get_candle_interval(ohlc) # mt5
    obj = (pd.to_datetime(ohlc.index[-1]) - pd.to_datetime(ohlc.index[0])).components
    day = obj.days
    hour = obj.hours
    minutes = obj.minutes
    horizontals = len(resistance_lines)
    return f"{ticker}: candle {candle_interval}, start: {ohlc.index[0]}, " \
           f"timedelta: {day}d, {hour}h, {minutes}m. S/R: {horizontals}. Show weekends: {show_weeks}"


def fig_title_for_futures_open_interest(ticker, ohlc):
    candle_interval = get_candle_interval(ohlc) # open interest binance futures
    obj = (pd.to_datetime(ohlc.index[-1]) - pd.to_datetime(ohlc.index[0])).components
    day = obj.days
    hour = obj.hours
    minutes = obj.minutes
    return f'{ticker}PERP: open_interest interval {candle_interval}, start: {ohlc.index[0]}, ' \
           f'timedelta: {day}d, {hour}h, {minutes}m.'


def fig_title_for_futures_with_one_ticker(ticker, ohlc, resistance_lines):
    candle_interval = get_candle_interval(ohlc)  # for spot and binance_spot
    obj = (pd.to_datetime(ohlc.index[-1]) - pd.to_datetime(ohlc.index[0])).components
    day = obj.days
    hour = obj.hours
    minutes = obj.minutes
    horizontals = len(resistance_lines)
    return f"{ticker}: candle {candle_interval}, start: {ohlc.index[0]}, timedelta: {day}d, {hour}h, {minutes}m. S/R: {horizontals}"


def fig_title_for_futures_with_reference(ticker, ref, ohlc, plot_non_trading):
    candle_interval = get_candle_interval(ohlc)  # spot futures reference
    obj = (pd.to_datetime(ohlc.index[-1]) - pd.to_datetime(ohlc.index[0])).components
    day = obj.days
    hour = obj.hours
    minutes = obj.minutes
    return f"{ticker} vs {ref}: candle {candle_interval}, start: {ohlc.index[0]}, " \
           f"timedelta: {day}d, {hour}h, {minutes}m. Plot non trading days: {plot_non_trading}."


def horizontal_lines_colors(resistances, boundaries):
    colors = []
    for price in resistances:
        colors.append('blue')
    for price in boundaries:
        colors.append('black')
    return colors


def volume_usd_column_for_ohlc(ohlc):
    return ((ohlc['open'] + ohlc['close'] / 2) * ohlc['volume']).apply(lambda x: int(round(x, 0)))


def cut_frames_regarding_date(oi, c):
    lower_bound = max([c.index.min(), oi.index.min()])
    upper_bound = min([c.index.max(), oi.index.max()])

    oi = oi[(oi.index > lower_bound) & (oi.index < upper_bound)]
    c = c[(c.index > lower_bound) & (c.index < upper_bound)]
    return oi, c


def last_tweet(webpage, api):
    screen_name = webpage.rsplit('com/')[1]
    try:
        resp = api.user_timeline(screen_name=screen_name)[00].created_at
        resp = datetime.datetime.strptime(resp.strftime('%Y-%m-%d %H:%M:%S'), '%Y-%m-%d %H:%M:%S')
    except tweepy.errors.Unauthorized:
        resp = None
    except tweepy.errors.NotFound:
        resp = None
    except IndexError:
        resp = None
    return resp


def print_timedelta(fr):
    obj = (pd.to_datetime(fr.date_x.iloc[0]) - pd.to_datetime(fr.date_y.iloc[0])).components
    day = obj.days
    hour = obj.hours
    minutes = obj.minutes
    seconds = obj.seconds
    print(f"{datetime_desired_format(datetime.datetime.now())}. "
          f"Timedelta: {day} days {hour} hours {minutes} minutes {seconds} seconds.")
    return


def check_string(s):
    flag_l = False
    if s == 'None':
        return False
    try:
        for i in s:
            if i.isalpha():
                flag_l = True
                return flag_l
            else:
                flag_l = False
    except TypeError:
        return False
    return flag_l


def download_coingecko_api_ids():
    api_ids_data = get_json('https://api.coingecko.com/api/v3/coins/list', True)
    return pd.DataFrame(api_ids_data)


def datetime_desired_format(t):
    return t.strftime('%Y-%m-%d %H:%M:%S')


def get_json(website_address: str, to_json=True):
    while True:
        try:
            if to_json is False:
                return requests.get(website_address, headers=agents.scrap()).text
            else:
                return requests.get(website_address, headers=agents.scrap()).json()
        except ConnectionError:
            print(f'{datetime_desired_format(datetime.datetime.now())}. ConnectionError in dp.get_json().')
            time.sleep(60)
            continue


def get_support_and_resistance(df, param=None):
    levels = []
    for i in range(2, df.shape[0] - 2):
        if is_support(df, i):
            levels.append((i, df['low'][i]))
        elif is_support(df, i):
            levels.append((i, df['high'][i]))
    s = np.mean(df['high'] - df['low'])
    levels = []
    for i in range(2, df.shape[0] - 2):
        if is_support(df, i):
            ll = df['low'][i]
            if is_far_from_level(ll, s, levels, param):
                levels.append((i, ll))
        elif is_resistance(df, i):
            ll = df['high'][i]
            if is_far_from_level(ll, s, levels, param):
                levels.append((i, ll))
    price_vals = []
    for level in levels:
        price_vals.append(level[1])
    return price_vals


def is_support(df, i):
    return (df['low'][i] < df['low'][i-1]) and (df['low'][i] < df['low'][i+1]) and (df['low'][i+1] < df['low'][i+2]) \
              and (df['low'][i-1] < df['low'][i-2])


def is_resistance(df, i):
    return (df['high'][i] > df['high'][i-1]) and (df['high'][i] > df['high'][i+1]) \
                 and (df['high'][i+1] > df['high'][i+2]) and (df['high'][i-1] > df['high'][i-2])


def is_far_from_level(ll, s, levels, param):
    if param is None:
        return np.sum([abs(ll - x) < 5 * s for x in levels]) == 0
    else:
        return np.sum([abs(ll - x) < param * s for x in levels]) == 0


def amount_of_candles_per_interval(interval, candle_interval, binance=True, open_interest=False):
    if 'd' in interval:
        if 'w' in candle_interval:
            amount = int(candle_interval[:-1]) * int(interval[:-1]) / 5
        elif 'd' in candle_interval:
            amount = int(candle_interval[:-1]) * int(interval[:-1])
        elif 'h' in candle_interval:
            amount = 24/int(candle_interval[:-1]) * int(interval[:-1])
        elif 'm' in candle_interval:
            amount = 1440/int(candle_interval[:-1]) * int(interval[:-1])
        else:
            raise TypeError()
    elif 'h' in interval:
        if 'h' in candle_interval:
            amount = int(interval[:-1])
        elif 'm' in candle_interval:
            amount = 60/int(candle_interval[:-1]) * int(interval[:-1])
        else:
            raise TypeError()
    else:
        raise TypeError()
    if binance is not False:
        if int(amount) > 1000:
            return 1000
    if open_interest is True:
        if int(amount) > 500:
            return 500
    return int(amount)


def get_api_ids():
    actual_cg_api_ids = list(download_coingecko_api_ids().id)
    psql_api_ids = list(executions.coingecko_listed_projects().api_id)
    return actual_cg_api_ids, psql_api_ids


def get_debank_wallets_transactions(wallet_addres):
    url = f'https://pro-openapi.debank.com/v1/user/all_history_list?id={wallet_addres}'
    data = requests.request("get", url, headers=credentials.debank_api_key()).json()
    return data


def convert_to_float(x):
    try:
        return float(x)
    except ValueError:
        return x


def convert_index_to_native(df):
    if df.index.tzinfo is not None:
        df.index = df.index.tz_localize(None)

    return df


def open_url_in_jupyter(url):
    return webbrowser.open(url)


if __name__ == '__main__':
    #intervals = {'100d': '1h', '30d': '1h'}
    #intervals = None
    w = get_debank_wallets_transactions('0xbb030a0408edc41f3c61ba3f132c06e93eda68f4')
