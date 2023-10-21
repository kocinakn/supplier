from tools import data_preparation as dp
from IPython.display import display
from postgres import modifications, executions, queries
import pandas as pd
import datetime
import warnings
import time

warnings.filterwarnings("ignore")
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', None)
pd.options.display.float_format = '{:20,.5f}'.format


class WalletsTracker:
    def __init__(self, save_transactions, apply_time_threshold):
        self.starting_time = datetime.datetime.now()
        self.apply_time_threshold = apply_time_threshold
        self.wallets = executions.wallets_tracked_addresses()
        self.responses = executions.wallets_tracked_responses()
        self.date_threshold_for_report = executions.wallets_tracker_threshold().iloc[0, 0]
        self.time_threshold = executions.wallets_tracker_threshold().iloc[0, 0]
        self.former_activities = executions.wallets_activities()
        self.activities = self.track_wallets()

        if save_transactions:
            self.save_activities()
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Activities above saved in db.')

            modifications.update_db(queries.update_wallets_tracker_threshold(self.starting_time))
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. New time threshold saved in db.')

    def track_wallets(self):
        wallets_tracked = self.wallets[self.wallets['public'] == False]
        df = None
        for index, row in wallets_tracked.iterrows():
            self.print_info(row)
            data = dp.get_debank_wallets_transactions(row["wallet_addres"])
            table = self.dataframe_from_wallet_data(data, row["wallet_addres"])
            if df is None:
                df = table
            else:
                df = pd.concat([df, table])

            self.print_tables(table, row['wallet_description'])
            time.sleep(5)
        return df.reset_index(drop=True)

    @staticmethod
    def print_info(row):
        tt = dp.datetime_desired_format(datetime.datetime.now())
        website = f'https://debank.com/profile/{row["wallet_addres"]}'
        print(f'{tt}. {row["wallet_description"]}: {row["wallet_addres"]}')
        print(f'{tt}. {website}')

    def dataframe_from_wallet_data(self, data, wallet_addres):
        tokens_table = self.get_tokens_table(data['token_dict'])
        wallet_history = self.get_wallet_history(tokens_table, data['history_list'], wallet_addres)
        if self.apply_time_threshold:
            if wallet_history.empty is False:
                wallet_history = wallet_history[wallet_history['event_time'] > self.time_threshold]
        return wallet_history

    def get_tokens_table(self, tokens_data):
        df = pd.DataFrame()
        if len(tokens_data.keys()) == 0:
            return None
        for identification, data in tokens_data.items():
            df = pd.concat([df, self.get_token_info(identification, data)])
        df.columns = ['chain', 'symbol', 'name', 'price', 'coin_id']
        return df.reset_index(drop=True)

    def get_token_info(self, coin_identification, data):
        chain = self.get_chain_name(data['chain'])
        coin_id = coin_identification
        name = self.get_name_for_tokens_table(data)
        symbol = self.get_symbol_for_tokens_table(data)
        price = self.get_price_for_tokens_table(data)
        if len(data) == 16:
            symbol = self.get_name_for_tokens_table(data)  # to have nft name in line with crypto token symbols
        return pd.DataFrame([chain, symbol, name, price, coin_id]).T

    @staticmethod
    def get_name_for_tokens_table(data):
        try:
            return data['name']
        except IndexError:
            return None

    @staticmethod
    def get_symbol_for_tokens_table(data):
        try:
            return data['symbol']
        except IndexError:
            return None

    @staticmethod
    def get_price_for_tokens_table(data):
        try:
            price = data['price']
            if price is None:
                price = 0
        except IndexError:
            price = 0
        except KeyError:
            price = 0
        return price

    def get_wallet_history(self, tokens_ids, transactions, wallet_addres):
        df = pd.DataFrame()
        for transaction in transactions:
            if (len(transaction['sends']) + len(transaction['receives'])) == 0:
                continue
            else:
                if self.apply_time_threshold:
                    interaction_date = pd.to_datetime(int(transaction['time_at']), unit='s')
                    if interaction_date < self.date_threshold_for_report:
                        continue
                d = self.transaction_type(tokens_ids, transaction, wallet_addres)
                df = pd.concat([df, d])
        df = self.wallet_history_df(df)
        return df

    def transaction_type(self, tokens_ids, transaction, wallet_addres):
        if transaction['cate_id'] == 'receive' or transaction['cate_id'] == 'send':
            return self.transfer_details(tokens_ids, transaction, wallet_addres)
        else:
            return self.interaction_details(tokens_ids, transaction, wallet_addres)

    def transfer_details(self, tokens_ids, transaction, wallet_addres):
        psql_id = ''
        addres = wallet_addres
        action = self.transfer_action(transaction)
        chain = self.get_chain_name(transaction['chain'])
        sends, send_value, receives, receives_value = 0, 0, 0, 0
        if len(transaction['sends']) == 0:
            receives, receives_value = self.get_receivings(tokens_ids, transaction)
        if len(transaction['receives']) == 0:
            sends, send_value = self.get_sendings(tokens_ids, transaction)
        counterparty = self.get_transfer_counterparty(transaction)
        transaction_date = self.event_time(transaction)
        r = [psql_id, addres, action, chain, sends, send_value, receives,
             receives_value, counterparty, transaction_date]
        return pd.DataFrame(r).T

    def interaction_details(self, tokens_ids, transaction, wallet_addres):
        psql_id = ''
        addres = wallet_addres
        action = self.get_action(transaction)
        chain = self.get_chain_name(transaction['chain'])
        sends, send_value = self.get_sendings(tokens_ids, transaction)
        receives, receives_value = self.get_receivings(tokens_ids, transaction)
        counterparty = 'no_counterparty'
        transaction_date = self.event_time(transaction)
        r = [psql_id, addres, action, chain, sends, send_value,
             receives, receives_value, counterparty, transaction_date]
        return pd.DataFrame(r).T

    def get_sendings(self, tokens_ids, transaction):
        sends = 0
        amount = 0
        for s in transaction['sends']:
            token_symbol = self.get_symbol(tokens_ids, s)
            amount = round(s['amount'], 6)
            if sends == 0:
                sends = str(amount) + ' ' + token_symbol
            else:
                sends = sends + f"+ {str(amount) + ' ' + token_symbol}"
        price = self.get_price(tokens_ids, transaction, 'sends')
        return sends, self.get_value(price, amount)

    def get_receivings(self, tokens_ids, transaction):
        receives = 0
        amount = 0
        for s in transaction['receives']:
            token_symbol = self.get_symbol(tokens_ids, s)
            amount = self.get_amount(s)
            if receives == 0:
                receives = str(amount) + ' ' + token_symbol
            else:
                receives = receives + f"+ {str(amount) + ' ' + token_symbol}"
        price = self.get_price(tokens_ids, transaction, 'receives')
        return receives, self.get_value(price, amount)

    @staticmethod
    def get_amount(s):
        amount = s['amount']
        if amount is None:
            amount = '0'
        else:
            amount = round(amount, 6)
        return amount

    @staticmethod
    def event_time(info):
        try:
            return datetime.datetime.fromtimestamp(int(info['time_at']))
        except IndexError:
            return None

    @staticmethod
    def get_symbol(token_ids, trade):
        try:
            return token_ids[token_ids['coin_id'] == trade['token_id']].symbol.iloc[0]
        except IndexError:
            return '0'

    @staticmethod
    def get_price(token_ids, info, transfer_type):
        try:
            return token_ids[token_ids['coin_id'] == info[transfer_type][0]['token_id']].price.iloc[0]
        except IndexError:
            return 0

    @staticmethod
    def get_value(price, amount):
        try:
            return round(price * amount, 3)
        except IndexError:
            return 0
        except KeyError:
            return 0

    def get_transfer_counterparty(self, info):
        try:
            desc = self.wallets[self.wallets['wallet_addres'] == info['other_addr']].wallet_description.iloc[0]
            return desc
        except IndexError:
            try:
                return info['other_addr']
            except IndexError:
                return None
        except ValueError:
            try:
                return info['other_addr']
            except IndexError:
                return None
        except KeyError:
            try:
                return info['other_addr']
            except IndexError:
                return None

    @staticmethod
    def wallet_history_df(df):
        try:
            df.columns = ['id', 'wallet_addres', 'action', 'chain', 'sends', 'sends_value', 'receives', 'receives_value',
                          'counterparty', 'event_time']
        except ValueError:
            pass
        return df.reset_index(drop=True)

    def print_tables(self, table, wallet_description):
        if table.empty:
            print(f'No transaction after {self.date_threshold_for_report} for {wallet_description}.')
            print('\n\n\n\n')
        else:
            try:
                table = table[['action', 'chain', 'sends', 'sends_value',
                               'receives', 'receives_value', 'counterparty', 'event_time']]
                display(table)
                print('\n\n\n\n')
            except KeyError:
                print('\n\n\n\n')
                pass

    def get_action(self, transaction):
        try:
            x = transaction['tx']['name']
            if len(x) == 0:
                return 'No Tx Data'
            try:
                return self.responses[self.responses['response'] == x].description.iloc[0]
            except KeyError:
                print(f'{dp.datetime_desired_format(datetime.datetime.now())}. New transfer method: {x}')
                return x
            except IndexError:
                print(f'{dp.datetime_desired_format(datetime.datetime.now())}. New transfer method: {x}')
                return x
        except IndexError:
            print(transaction['tx'])
            return 'No Tx Data'
        except TypeError:
            return 'No Tx Data'

    def get_chain_name(self, chain_id):
        try:
            d = self.responses[self.responses['response'] == chain_id].description.iloc[0]
            return d
        except IndexError:
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. New chain to dictionary: {chain_id}')
            return chain_id
        except KeyError:
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. New chain to dictionary: {chain_id}')
            return chain_id

    @staticmethod
    def transfer_action(transaction):
        d = {'receive': 'Receive',
             'send': 'Send'}
        try:
            return d[transaction['cate_id']]
        except IndexError:
            return transaction['cate_id']
        except KeyError:
            return transaction['cate_id']

    def save_activities(self):
        try:
            last_id = max(self.former_activities['id']) + 1
        except ValueError:
            last_id = 0
        for index, row in self.activities.iterrows():
            self.activities.loc[index, 'id'] = last_id
            last_id = last_id + 1
        modifications.save_df_to_psql(self.activities, 'wallets_activities', 'append')
        return


if __name__ == '__main__':
    WalletsTracker(save_transactions=False, apply_time_threshold=False)
