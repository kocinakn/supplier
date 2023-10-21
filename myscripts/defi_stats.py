from tools import data_preparation as dp
from postgres import executions, modifications
from IPython.display import display

import pandas as pd
import datetime
import warnings

warnings.filterwarnings("ignore")
pd.set_option('display.width', 500)
pd.set_option('display.max_columns', 15)
pd.set_option('display.max_rows', None)
pd.options.display.float_format = '{:20,.5f}'.format


class DefiDappsStats:
    def __init__(self, save_in_db=True):
        t0 = datetime.datetime.now()
        self.save_in_db = save_in_db
        self.former = executions.defi_dapps_market()
        self.actual = dp.defillama_protocols()
        self.comparing = self.merge_actual_with_former()
        self.stats = self.defi_dapps_table_with_stats()

        if self.save_in_db:
            modifications.save_dataframe_in_postgres(self.actual, 'defi_dapps_market', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. DefiDappsStats: current market data saved in db.')

            modifications.save_dataframe_in_postgres(self.stats, 'defi_dapps_stats', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. DefiDappsStats: stats saved in db.')
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Report created in: {datetime.datetime.now() - t0}.')

    def merge_actual_with_former(self):
        return self.actual.merge(self.former, on=['name'],
                                 how='left').apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1).dropna()

    def defi_dapps_table_with_stats(self):
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. DefiDappsStats: {self.comparing.date_y.iloc[0]} - '
              f'{self.comparing.date_x.iloc[0]}.')
        dp.print_timedelta(self.comparing)
        fr = self.calculate_stats()
        return self.prepared_calculated_stats(fr)

    def calculate_stats(self):
        self.comparing['tvl_perc'] = (self.comparing.tvl_x - self.comparing.tvl_y) / self.comparing.tvl_y * 100
        self.comparing['mcap_perc'] = (self.comparing.mcap_x - self.comparing.mcap_y) / self.comparing.mcap_y * 100
        fr = self.comparing[['symbol_x', 'name', 'gecko_id_x', 'category_y', 'tvl_x', 'tvl_y', 'mcap_x', 'tvl_perc',
                             'mcap_perc', 'chains_x', 'chainTvls_x', 'date_x', 'date_y']]
        fr.rename(columns={'category_y': 'category'}, inplace=True)
        fr.columns = [col.replace('_x', '') for col in fr.columns]
        return fr

    @staticmethod
    def prepared_calculated_stats(fr):
        not_included = ['CEX', 'Chain']
        print(f"{dp.datetime_desired_format(datetime.datetime.now())}. Categories not included: {not_included}.")
        fr = fr[~fr['category'].isin(not_included)]
        return fr.sort_values('tvl', ascending=False).fillna(0).reset_index(drop=True)

    def check_for_new_protocols(self):
        df_all = self.former.sort_values('name', ascending=True).\
            merge(self.actual.sort_values('name', ascending=True), on=['name'], how='left', indicator=True)
        try:
            new = df_all[df_all._merge == 'left_only']
            new = new.drop(new.filter(regex='_y').columns, axis=1)
            new.columns = new.columns.str.rstrip('_x')
        except KeyError:
            print(f"{dp.datetime_desired_format(datetime.datetime.now())}. No new defi_dapps found.")
            new = pd.DataFrame()
        return new.iloc[:, :-1].reset_index(drop=True)


class TvlPerChainStats:
    def __init__(self, save_in_db=True):
        t0 = datetime.datetime.now()
        self.save_in_db = save_in_db
        self.former = executions.tvl_per_chain_market()
        self.actual = dp.defillama_chains()
        self.comparing = self.merge_actual_with_former()
        self.stats = self.tvl_per_chain_table_with_stats(self.comparing.fillna(0))

        if self.save_in_db:
            modifications.save_dataframe_in_postgres(self.actual, 'tvl_per_chain_market', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. TvlPerChainStats: current market data saved in db.')

            modifications.save_dataframe_in_postgres(self.stats, 'tvl_per_chain_stats', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. TvlPerChainStats: stats saved in db.')
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Report created in: {datetime.datetime.now() - t0}.')

    def merge_actual_with_former(self):
        return self.actual.merge(self.former, on=['name'],
                                 how='left').apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1)

    def tvl_per_chain_table_with_stats(self, fr):
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. TvlPerChainStats: {fr.date_y.iloc[0]} - {fr.date_x.iloc[0]}.')
        dp.print_timedelta(fr)
        return self.calculate_stats(fr)

    @staticmethod
    def calculate_stats(fr):
        fr['tvl_perc'] = (fr.tvl_x - fr.tvl_y) / fr.tvl_y * 100
        fr['share_perc'] = (fr.share_x - fr.share_y) / fr.share_y * 100
        fr = fr[['tokenSymbol_x', 'name', 'gecko_id_x', 'tvl_x', 'tvl_y', 'share_x',
                 'share_y', 'tvl_perc', 'share_perc', 'date_x', 'date_y']]
        fr.columns = [col.replace('_x', '') for col in fr.columns]
        return fr.sort_values('tvl_perc', ascending=False).reset_index(drop=True).fillna(0)


class StablecoinsStats:
    def __init__(self, save_in_db=True):
        t0 = datetime.datetime.now()
        self.save_in_db = save_in_db
        self.former = executions.stablecoins_market()
        self.actual = dp.defillama_stablecoins()
        self.comparing = self.merge_actual_with_former()
        self.stats = self.stablecoins_table_with_stats(self.comparing.fillna(0))

        if self.save_in_db:
            modifications.save_dataframe_in_postgres(self.actual, 'stablecoins_market', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. StablecoinsStats: current market data saved in db.')

            modifications.save_dataframe_in_postgres(self.stats, 'stablecoins_stats', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. StablecoinsStats: stats saved in db.')
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Report created in: {datetime.datetime.now() - t0}.')

    def merge_actual_with_former(self):
        return self.actual.merge(self.former, on=['gecko_id'],
                                 how='left').apply(lambda x: x.apply(lambda y: dp.ret_float(y)), axis=1)

    def stablecoins_table_with_stats(self, fr):
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. StablecoinsStats: {fr.date_y.iloc[0]} - {fr.date_x.iloc[0]}.')
        dp.print_timedelta(fr)
        return self.calculate_stats(fr)

    @staticmethod
    def calculate_stats(fr):
        fr['circ_perc'] = (fr.circulating_x - fr.circulating_y) / fr.circulating_y * 100
        fr['price_perc'] = (fr.price_x - fr.price_y) / fr.price_y * 100

        fr = fr[['symbol_x', 'name_x', 'gecko_id', 'price_x', 'price_y', 'circulating_x', 'circulating_y',
                 'price_perc', 'circ_perc', 'chains_x', 'date_x', 'date_y']]
        fr.columns = [col.replace('_x', '') for col in fr.columns]
        return fr.sort_values('circulating', ascending=False).reset_index(drop=True).fillna(0)


class DefiOnGivenChain:
    def __init__(self, tvl_threshold=None, comparing_table='DefiOnGivenChain'):
        self.tvl_threshold = tvl_threshold
        self.comparing_table = comparing_table
        self.former = self.pick_postgres_table()
        self.actual = dp.defillama_protocols()
        self.chains = self.get_chains()

    def pick_postgres_table(self):
        if self.comparing_table == 'DefiOnGivenChain':
            df = executions.defi_on_given_chain()
        else:
            df = executions.defi_dapps_market()
        return df

    def get_chains(self):
        chains = []
        for index, row in self.actual.iterrows():
            chain_list = row['chains']
            for chain in chain_list:
                chains.append(chain)
        return sorted(set(chains))

    def prepare_former_table_to_compare(self, chain):
        fr = self.former
        fr['app_tvl_on_chain'] = ''
        for index, row in fr.iterrows():
            if chain in row['chains']:
                try:
                    fr.loc[index, 'app_tvl_on_chain'] = eval(row['chainTvls'])[chain]
                except KeyError:
                    fr.loc[index, 'app_tvl_on_chain'] = 0
                continue
            else:
                fr = fr.drop(index)
        return fr

    def prepare_actual_table_to_compare(self, chain):
        df = self.actual
        for index, row in df.iterrows():
            if chain in row['chains']:
                try:
                    df.loc[index, 'app_tvl_on_chain'] = row['chainTvls'][chain]
                except KeyError:
                    df.loc[index, 'app_tvl_on_chain'] = 0
                continue
            else:
                df = df.drop(index)
        for index, row in df.iterrows():
            try:
                df.loc[index, 'chain_share'] = round(float(row['app_tvl_on_chain'] / row['tvl']), 5)
            except ZeroDivisionError:
                df.loc[index, 'chain_share'] = 0
        return df

    def prepare_merged_table(self, fr, df):
        merged = df.merge(fr, on=['name'], how='left')
        merged['app_tvl_on_chain_perc'] = 0.0
        for index, row in merged.iterrows():
            try:
                merged.loc[index, 'app_tvl_on_chain_perc'] = float((row['app_tvl_on_chain_x'] - row['app_tvl_on_chain_y']) / row['app_tvl_on_chain_y']) * 100
            except ZeroDivisionError:
                merged.loc[index, 'app_tvl_on_chain_perc'] = 0
        merged = merged[['name', 'symbol_x', 'gecko_id_x', 'category_x', 'tvl_x',
                         'tvl_y', 'app_tvl_on_chain_x', 'app_tvl_on_chain_y', 'app_tvl_on_chain_perc',
                         'chain_share', 'mcap_x', 'chains_x', 'date_x', 'date_y']]. \
            sort_values(['chain_share', 'tvl_x'], ascending=False).reset_index(drop=True)
        merged.columns = [col.replace('_x', '') for col in merged.columns]
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Amount of projects: {len(merged)}')
        if self.tvl_threshold is not None:
            amount_before_threshold = len(merged)
            merged = merged[merged['app_tvl_on_chain'] > dp.string_number_to_integer(self.tvl_threshold)]
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
                  f'Dropped projects because of tvl_threshold: {amount_before_threshold - len(merged)}')
        return merged.reset_index(drop=True)

    def dapps_in_chain(self):
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Chains: {len(self.chains)}')
        print('\n')
        dt = {}
        while True:
            print(self.chains[:])
            chain = input(f'What chain are you interested in? Press e to finish.')
            if chain == 'e':
                dt[chain] = None
                break
            fr = self.prepare_former_table_to_compare(chain)
            df = self.prepare_actual_table_to_compare(chain)
            merged = self.prepare_merged_table(fr, df)
            display(merged)
            dt[chain] = merged
        return dt

    def save_alternative_dapps_data(self):
        answer = input('Save in db currently prepared defi dapps data (alternative to DefiDappsStats)? Type y if yes.')
        if answer == 'y':
            modifications.save_dataframe_in_postgres(self.actual, 'defi_on_given_chain', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
                  f'DefiOnGivenChain: current market data saved in db.')
            return
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
              f'DefiOnGivenChain: current market data not saved in db.')
        return


if __name__ == '__main__':
    t = DefiOnGivenChain(tvl_threshold='500 000', comparing_table='DefiOnGivenChain')
