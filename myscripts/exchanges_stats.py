from tools import data_preparation as dp
from tools import connections
from postgres import executions, modifications, queries

import pandas as pd
import datetime
import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', None)
pd.options.display.float_format = '{:20,.5f}'.format


class BinanceFuturesStats:
    def __init__(self, save_watchlist, save_in_db, main_quote_currency='USDT', secondary_quote_currency='BUSD',
                 turnover_threshold=None):
        t = datetime.datetime.now()
        self.save_watchlist = save_watchlist
        self.save_in_db = save_in_db
        self.turnover_threshold = turnover_threshold
        self.main_quote_currency = main_quote_currency
        self.secondary_quote_currency = secondary_quote_currency
        self.client = connections.connect_binance_futures_api()
        self.former = executions.binance_futures_markets()
        self.actual = self.binance_futures_market_data()
        self.funding = self.funding_table()
        self.comparing = self.merge_actual_with_former()
        self.stats = self.binance_futures_table_with_stats(self.comparing)
        self.symbols_in_play = self.prepare_watchlist()

        if self.save_in_db:

            modifications.save_dataframe_in_postgres(self.actual, 'binance_futures_market', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Market data saved in db.')

            modifications.save_dataframe_in_postgres(self.stats, 'binance_futures_stats', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Stats saved in db.')

        if self.save_watchlist:
            modifications.update_db(queries.delete_previous_watchlist_tickers('BINANCE_FUTURES'))
            modifications.save_dataframe_in_postgres(self.symbols_in_play, 'symbols_in_play', 'append')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Watchlist saved in db.')

        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Report created in: {datetime.datetime.now() - t}.')

    def binance_futures_market_data(self):
        symbols = dp.binance_futures_symbols(self.client, self.main_quote_currency, self.secondary_quote_currency)
        fr = self.market_data_table(symbols)
        return fr

    def market_data_table(self, symbols):
        df = None
        symbols_stats = dp.download_binance_futures_24h_stats(self.client) # one hit to API instead of 200+
        for symbol in symbols:
            symbol_stats = symbols_stats[symbols_stats['symbol'] == symbol]
            mp = dp.download_binance_futures_ticker_price(self.client, symbol)
            oi = dp.download_binance_futures_open_interest(self.client, symbol, '1h', '5m')[-1]
            df = self.market_data(df, symbol_stats, mp, oi)
        df.columns = ['ticker', 'mark_price', 'turnover', 'oi_usd', 'open_interest', 'trades',
                      'trad/min', 'funding', 'date']
        return df.sort_values('oi_usd', ascending=False).reset_index(drop=True)

    @staticmethod
    def market_data(df, symbol_stats, mp, oi):
        mark_price = float(mp['markPrice'])
        turnover = round(float(symbol_stats['quoteVolume']))
        oi_usd = round(float(oi['sumOpenInterestValue']))
        open_interest = round(float(oi['sumOpenInterest']))
        trades = symbol_stats['count'].iloc[0]
        freq = round(float((symbol_stats['count'] / (((symbol_stats['closeTime'] - symbol_stats['openTime']) / 1000) / 60)).iloc[0]))
        funding = float(mp['lastFundingRate'])
        date = dp.datetime_desired_format(datetime.datetime.now())
        row = [symbol_stats['symbol'].iloc[0], mark_price, turnover, oi_usd, open_interest, trades, freq, funding, date]
        if df is None:
            df = pd.DataFrame(row).T
        else:
            df = pd.concat([df, pd.DataFrame(row).T])
        return df

    def funding_table(self):
        return self.actual.sort_values('funding', ascending=True).reset_index(drop=True).\
            apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1)

    def merge_actual_with_former(self):
        return self.actual.merge(self.former, on=['ticker'],
                                 how='left').apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1).dropna()

    def binance_futures_table_with_stats(self, fr):
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. {fr.date_y.iloc[0]} - {fr.date_x.iloc[0]}.')
        dp.print_timedelta(fr)
        return self.calculate_stats(fr)

    @staticmethod
    def calculate_stats(fr):
        fr['price_perc'] = (fr.mark_price_x - fr.mark_price_y) / fr.mark_price_y * 100
        fr['open_interest_perc'] = (fr.open_interest_x - fr.open_interest_y) / fr.open_interest_y * 100
        fr['turnover_perc'] = (fr.turnover_x - fr.turnover_y) / fr.turnover_y * 100
        fr['trades_perc'] = (fr.trades_x - fr.trades_y) / fr.trades_y * 100
        fr = fr[['ticker', 'mark_price_x', 'oi_usd_x', 'open_interest_x', 'open_interest_y', 'turnover_x', 'trades_x',
                 'open_interest_perc', 'turnover_perc', 'trades_perc', 'price_perc', 'date_x', 'date_y']]
        fr.columns = [col.replace('_x', '') for col in fr.columns]
        return fr.sort_values('turnover', ascending=False).reset_index(drop=True)

    def prepare_watchlist(self):
        t1 = self.stats.sort_values('open_interest_perc', ascending=False).head()
        t2 = self.stats.sort_values('price_perc', ascending=False).head()
        df = pd.concat([t1, t2]).drop_duplicates().sort_values('turnover', ascending=False).reset_index(drop=True)
        df = df[['ticker', 'mark_price', 'turnover', 'date']].reset_index(drop=True)
        exchange = 'BINANCE_FUTURES'
        df['exchange'] = exchange
        df['plotted'] = False
        df.rename(columns={'mark_price': 'price'}, inplace=True)
        if self.turnover_threshold is not None:
            df = df[df['turnover'] > dp.string_number_to_integer(self.turnover_threshold)]
        return df[['exchange', 'ticker', 'price', 'turnover', 'plotted', 'date']]


class BinanceSpotStats:
    def __init__(self, save_watchlist, save_in_db, main_quote_currency='USDT', secondary_quote_currency='BUSD',
                 turnover_threshold=None):
        t = datetime.datetime.now()
        self.save_watchlist = save_watchlist
        self.save_in_db = save_in_db
        self.turnover_threshold = turnover_threshold
        self.main_quote_currency = main_quote_currency
        self.secondary_quote_currency = secondary_quote_currency
        self.client = connections.connect_binance_spot_api()
        self.former = executions.binance_spot_market()
        self.actual = self.binance_spot_market_data()
        self.comparing = self.merge_actual_with_former()
        self.stats = self.binance_spot_table_with_stats(self.comparing)
        self.to_report = self.stats_tailored_to_report(self.stats)
        self.symbols_in_play = self.prepare_watchlist()
        if self.save_in_db:

            modifications.save_dataframe_in_postgres(self.actual, 'binance_spot_market', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Market data saved in db.')

            modifications.save_dataframe_in_postgres(self.stats, 'binance_spot_stats', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Stats saved in db.')

        if self.save_watchlist:
            #modifications.update_db(queries.delete_previous_watchlist_tickers('BINANCE_SPOT'))
            #modifications.save_dataframe_in_postgres(self.symbols_in_play, 'symbols_in_play', 'append')
            #print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Watchlist saved in db.')
            print(self.symbols_in_play)
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Watchlist disabled in spot.')

        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Report created in: {datetime.datetime.now() - t}.')

    def binance_spot_market_data(self):
        symbols = dp.binance_spot_symbols(self.client, self.main_quote_currency, self.secondary_quote_currency)
        fr = self.market_data_table(symbols)
        return fr

    def market_data_table(self, symbols):
        symbols_stats = dp.binance_spot_market_table(self.client)
        for index, row in symbols_stats.iterrows():
            if row['symbol'] not in symbols:
                symbols_stats = symbols_stats.drop(index)
            else:
                continue
        symbols_stats.rename(columns={'symbol': 'ticker', 'lastPrice': 'price', 'quoteVolume': 'turnover', 'count': 'trades'}, inplace=True)
        return symbols_stats.sort_values('turnover', ascending=False).reset_index(drop=True)

    def merge_actual_with_former(self):
        return self.actual.merge(self.former, on=['ticker'],
                                 how='left').apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1).dropna()

    def binance_spot_table_with_stats(self, fr):
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. BinanceSpotStats: {fr.date_y.iloc[0]} - {fr.date_x.iloc[0]}.')
        dp.print_timedelta(fr)
        return self.calculate_stats(fr)

    @staticmethod
    def calculate_stats(fr):
        fr['price_perc'] = (fr.price_x - fr.price_y) / fr.price_y * 100
        fr['turnover_perc'] = (fr.turnover_x - fr.turnover_y) / fr.turnover_y * 100
        fr['trades_perc'] = (fr.trades_x - fr.trades_y) / fr.trades_y * 100
        fr = fr[['ticker', 'price_x', 'turnover_x', 'turnover_y', 'trades_x',
                 'turnover_perc', 'trades_perc', 'price_perc', 'date_x', 'date_y']]
        fr.columns = [col.replace('_x', '') for col in fr.columns]
        return fr.sort_values('turnover', ascending=False).reset_index(drop=True)

    def stats_tailored_to_report(self, df):
        if self.main_quote_currency == 'BUSD':
            for index, row in df.iterrows():
                if row['ticker'][-4:] != 'BUSD':
                    df = df.drop(index)
                else:
                    continue
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Only BUSD pairs included.')
        if self.turnover_threshold is not None:
            df = df[df['turnover'] > dp.string_number_to_integer(self.turnover_threshold)]
        return df

    def prepare_watchlist(self):
        t1 = self.to_report.sort_values('turnover_perc', ascending=False).head()
        t2 = self.to_report.sort_values('price_perc', ascending=False).head().reset_index(drop=True)
        df = pd.concat([t1, t2]).drop_duplicates().sort_values('turnover', ascending=False).reset_index(drop=True)
        df = df[['ticker', 'price', 'turnover', 'date']]
        exchange = 'BINANCE_SPOT'
        df['exchange'] = exchange
        df['plotted'] = False
        return df[['exchange', 'ticker', 'price', 'turnover', 'plotted', 'date']]


class KucoinSpotStats:
    def __init__(self, save_in_db, main_quote_currency='USDT', secondary_quote_currency='USDC'):
        t = datetime.datetime.now()
        self.main_quote_currency = main_quote_currency
        self.secondary_quote_currency = secondary_quote_currency
        self.save_in_db = save_in_db
        self.former = executions.kucoin_spot_market()
        self.actual = self.kucoin_spot_market_data()
        self.comparing = self.merge_actual_with_former()
        self.stats = self.kucoin_spot_table_with_stats(self.comparing)
        if self.save_in_db:

            modifications.save_dataframe_in_postgres(self.actual, 'kucoin_spot_market', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. KucoinSpotStats: current market data saved in db.')

            modifications.save_dataframe_in_postgres(self.stats, 'kucoin_spot_stats', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. KucoinSpotStats: stats saved in db.')

        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Report created in: {datetime.datetime.now() - t}.')

    def kucoin_spot_market_data(self):
        df = dp.kucoin_spot_symbols(self.main_quote_currency, self.secondary_quote_currency)
        df = self.market_data_table(df)
        return df

    @staticmethod
    def market_data_table(df):
        data = pd.DataFrame(df).apply(lambda x: x.apply(lambda y: dp.ret_float(y)))
        data.rename(columns={'last': 'price', 'volValue': 'turnover', 'symbol': 'ticker'}, inplace=True)
        data['date'] = dp.datetime_desired_format(datetime.datetime.now())
        return data[['ticker', 'price', 'turnover', 'date']].sort_values('turnover', ascending=False).reset_index(
            drop=True)

    def merge_actual_with_former(self):
        return self.actual.merge(self.former, on=['ticker'],
                                 how='left').apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1)

    def kucoin_spot_table_with_stats(self, fr):
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. KucoinSpotStats: {fr.date_y.iloc[0]} - {fr.date_x.iloc[0]}.')
        dp.print_timedelta(fr)
        return self.calculate_stats(fr)

    @staticmethod
    def calculate_stats(fr):
        fr['price_perc'] = (fr.price_x - fr.price_y) / fr.price_y * 100
        fr['turnover_perc'] = (fr.turnover_x - fr.turnover_y) / fr.turnover_y * 100
        fr = fr[['ticker', 'price_x', 'price_y', 'turnover_x', 'turnover_y',
                 'price_perc', 'turnover_perc', 'date_x', 'date_y']]
        fr.columns = [col.replace('_x', '') for col in fr.columns]
        return fr.sort_values('turnover', ascending=False).reset_index(drop=True)
