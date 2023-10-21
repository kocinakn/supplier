from tools import data_preparation as dp
from postgres import executions, modifications
from IPython.display import display
import pandas as pd
import random
import time
import datetime
import warnings
warnings.filterwarnings("ignore")


def blacklisted_api_ids():
    return ['-fan-', '-synthetic-', '3x', 'aave-', '-avax', 'baby', 'bep20', '-bearing-', 'binance-peg', 'bitcoin-',
            '-bitcoin', 'blockchain', 'bnb-', '-bnb', 'btc', '-bsc', '-bridge', 'cat', 'celo-', '-club',
            'currency', 'curve-fi-', 'daddy', '-dai', 'decentralized', 'diamond', 'dog', 'elon', '-etf-', 'escrowed', '-eth',
            'floki', 'foundation', 'gold', 'huobi', 'idle-', 'index', 'iron-bank-', 'inu', 'investment', 'leverage',
            'liquid-', 'long', 'nftx', 'node', 'oec-', 'old', 'peg', 'reward',
            'rewards', 'safe', '-shards', 'shares', 'shib', 'short', 'silver', 'stader-', 'staked',
            'stock', 'tether-', '-tether', 'tokenized', 'terra-', '-usdt', 'vault', 'vehicle', 'venus', 'vote',
            'voting', 'wormhole', 'wrapped']


class CoingeckoListed:
    def __init__(self, save_in_db, info_about_new):
        t0 = datetime.datetime.now()
        print(f'{dp.datetime_desired_format(t0)}. CoinGecko new_api_ids started. Save in db: {save_in_db}. '
              f'Info_about_new {info_about_new}')
        self.save_in_db = save_in_db
        self.info_about_new = info_about_new
        self.server_side_api_ids = dp.download_coingecko_api_ids().rename(columns={'id': 'api_id'})
        self.new_ids = self.find_new_api_ids()
        self.filtered_ids = self.prepare_new_ids_dataframe_for_inserting_into_psql()

        if self.save_in_db:
            self.interact_with_psql()
        if self.info_about_new:
            show_api_ids_info(self.filtered_ids)
        print(f"{dp.datetime_desired_format(datetime.datetime.now())}. "
              f"CoinGecko new_api_ids finished. Duration: {datetime.datetime.now() - t0}.")

    def find_new_api_ids(self):
        new_ids = []
        psql_data = executions.coingecko_listed_projects()
        for index, row in self.server_side_api_ids.iterrows():
            if row['api_id'] not in list(psql_data.api_id):
                new_ids.append(row['api_id'])
            else:
                continue
        df = self.server_side_api_ids[self.server_side_api_ids['api_id'].isin(new_ids)]
        if len(df) > 0:
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. New api_ids: {len(df)}.')
        else:
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. No new api_ids.')
        return df

    def prepare_new_ids_dataframe_for_inserting_into_psql(self):
        df = self.columns_tailored_for_psql(self.new_ids).reset_index(drop=True)
        if df.empty is False:
            for index, row in df.iterrows():
                df.loc[index, 'symbol'] = row['symbol'].upper()
                banned_api_id = self.check_if_api_id_is_blacklisted(row)
                if banned_api_id is not False:
                    df.loc[index, 'description'] = f'banned({banned_api_id})'
                    df.loc[index, 'banned'] = True
                else:
                    df.loc[index, 'description'] = 'no_description'
                    df.loc[index, 'banned'] = False
            df = df[['symbol', 'api_id', 'rank', 'dex', 'dot', 'ibc', 'lend', 'lsd', 'l1', 'l2', 'nft', 'perps',
                               'pow', 'privacy', 'stables', 'yield', 'zk', 'banned', 'description']]
            display(df[['symbol', 'api_id', 'banned', 'description']])
        return df

    @staticmethod
    def columns_tailored_for_psql(new_ids):
        new_ids['dex'] = False
        new_ids['dot'] = False
        new_ids['ibc'] = False
        new_ids['lend'] = False
        new_ids['lsd'] = False
        new_ids['l1'] = False
        new_ids['l2'] = False
        new_ids['nft'] = False
        new_ids['perps'] = False
        new_ids['pow'] = False
        new_ids['privacy'] = False
        new_ids['stables'] = False
        new_ids['yield'] = False
        new_ids['zk'] = False
        new_ids['rank'] = 0
        new_ids['banned'] = None
        new_ids['description'] = None
        return new_ids

    @staticmethod
    def check_if_api_id_is_blacklisted(row):
        for banned_api_id in blacklisted_api_ids():
            if banned_api_id in row['api_id']:
                return banned_api_id
        return False

    def interact_with_psql(self):
        if len(self.new_ids) > 0:
            delete_expired_projects_from_db(self.server_side_api_ids)
            modifications.save_dataframe_in_postgres(self.filtered_ids, 'coingecko', 'append')
            print(f"{dp.datetime_desired_format(datetime.datetime.now())}. New api_ids saved in db.")
        return


def delete_expired_projects_from_db(actual_cg_api_ids=None):
    print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Checking for evaporated projects.')
    to_delete = []
    if actual_cg_api_ids is not None:
        psql_api_ids = list(executions.coingecko_listed_projects().api_id)
        actual_cg_api_ids = list(actual_cg_api_ids.api_id)
    else:
        actual_cg_api_ids, psql_api_ids = dp.get_api_ids()

    for api_id in psql_api_ids:
        if api_id in actual_cg_api_ids:
            continue
        else:
            to_delete.append(api_id)
    rows_deleted = 0
    if len(to_delete) == 0:
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. No evaporated projects.')
        return
    else:
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. api_ids to delete: {sorted(to_delete)[:]}')
        for name in to_delete:
            q = "DELETE FROM coingecko WHERE api_id = '{}'".format(name)
            modifications.update_db(q)
            rows_deleted = rows_deleted + 1
    print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Rows deleted: {rows_deleted}')
    return rows_deleted


def get_actual_data_for_api_id(api_id):
    resp = dp.download_coingecko_market_info_for_api_id(api_id)
    df = pd.DataFrame()

    df.loc[0, 'symbol'] = symbol_from_coingecko_response(resp)
    df.loc[0, 'name'] = name_from_coingecko_response(resp)
    df.loc[0, 'platform'] = platform_from_coingecko_response(resp)
    df.loc[0, 'category'] = category_from_coingecko_response(resp)

    market_data = market_data_from_coingecko_response(resp, df.loc[0, 'symbol'], df.loc[0, 'name'])
    if market_data is None:
        return df

    df.loc[0, 'price'] = extract_data_from_coingecko_response(market_data, 'current_price')
    df.loc[0, 'ath'] = extract_data_from_coingecko_response(market_data, 'ath')
    df.loc[0, 'ath_perc'] = extract_data_from_coingecko_response(market_data, 'ath_change_percentage')
    df.loc[0, 'ath_date'] = extract_data_from_coingecko_response(market_data, 'ath_date')
    df.loc[0, 'mcap'] = extract_data_from_coingecko_response(market_data, 'market_cap')
    df.loc[0, 'fdv'] = extract_data_from_coingecko_response(market_data, 'fully_diluted_valuation')

    df.loc[0, 'circ'] = coingecko_response_coingecko_coins_supply(market_data, 'circulating_supply')
    df.loc[0, 'max_supply'] = coingecko_response_coingecko_coins_supply(market_data, 'max_supply')
    df.loc[0, 'mcapfdv'] = coingecko_response_market_data_indicators(df.loc[0, 'mcap'], df.loc[0, 'fdv'])
    df.loc[0, 'circtotal'] = coingecko_response_market_data_indicators(df.loc[0, 'circ'], df.loc[0, 'max_supply'])
    df.loc[0, 'update_date'] = dp.datetime_desired_format(datetime.datetime.now())
    w = random.randint(15, 19)
    time.sleep(w)
    return df


def symbol_from_coingecko_response(resp):
    try:
        symbol = resp['symbol'].upper()
    except KeyError:
        symbol = None
    return symbol


def name_from_coingecko_response(resp):
    try:
        name = resp['name']
    except KeyError:
        name = None
    return name


def platform_from_coingecko_response(resp):
    platform = []
    try:
        data = resp['platforms']
        for k, v in data.items():
            platform.append(k)
    except IndexError:
        platform = platform
    except KeyError:
        platform = platform
    return str(platform)


def category_from_coingecko_response(resp):
    category = []
    try:
        for item in resp['categories']:
            if 'Ecosystem' in item:
                continue
            else:
                category.append(item.lower())
    except KeyError:
        return None
    if len(category) == 0:
        return None
    return str(category)


def market_data_from_coingecko_response(resp, symbol, name):
    try:
        return resp['market_data']
    except KeyError:
        print(f"{dp.datetime_desired_format(datetime.datetime.now())}. "
              f"{symbol} ({name}): KeyError resp['market_data'].")
        return None


def extract_data_from_coingecko_response(market_data, r):
    try:
        param = market_data[r]['usd']
        if r == 'ath_date':
            d = param[:10] + ' ' + param[11:19]
            param = datetime.datetime.strptime(d, '%Y-%m-%d %H:%M:%S')
    except KeyError:
        param = 0
    return param


def coingecko_response_market_data_indicators(param1, param2):
    try:
        param = round(param1 / param2, 4)
    except ZeroDivisionError:
        param = 0
    except TypeError:
        param = 0
    return param


def coingecko_response_coingecko_coins_supply(market_data, param):
    par = market_data[param]
    if par is None:
        par = 0
    return par


def show_api_ids_info(ids):
    if ids.empty is True:
        return
    df = pd.DataFrame()
    for index, row in ids.iterrows():
        try:
            d = get_actual_data_for_api_id(row['api_id'])
            if df.empty:
                df = d
            else:
                df = df.append(d)
        except AttributeError:
            continue
        except KeyError:
            continue
    display(df.reset_index(drop=True))
    print('\n')


if __name__ == '__main__':
    CoingeckoListed(save_in_db=True, info_about_new=False)
    pass
