from tools import data_preparation as dp
from tools import connections
from postgres import executions, modifications
import time
import tweepy.errors

import pandas as pd
import datetime

import warnings
warnings.filterwarnings("ignore")

# nie do użycia od 12.04.2023 - Elon ograniczył możliwości free-tier do 3 endpointów.


class TwitterFollowersMonitor:
    def __init__(self, save_in_db=True):
        t = datetime.datetime.now()
        self.save_in_db = save_in_db
        self.api = connections.connect_twitter_api()
        self.accounts_to_follow = list(executions.twitter_accounts_to_check().screen_name)
        self.former = executions.twitter_accounts()
        self.followers_data, self.selected_accounts = self.find_twitter_users_with_new_friends()
        self.show_new_followers()
        if self.save_in_db:

            modifications.save_dataframe_in_postgres(self.followers_data, 'twitter_accounts', 'replace')
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. TwitterFollowersMonitor: data saved in db.')

        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Report created in: {datetime.datetime.now() - t}')

    def find_twitter_users_with_new_friends(self):
        df = pd.DataFrame()
        i = 0
        for account in self.accounts_to_follow:
            df = self.iterate_over_accounts(df, account, i)
            i = i + 1
        df = self.prepare_accounts_df(df)
        merged = df.merge(self.former, on='name').apply(lambda x: x.apply(lambda y: dp.ret_int(y)), axis=1)
        merged['diff'] = merged['following_x'] - merged['following_y']
        selected_accounts = merged[merged['diff'] > 0]
        return df, selected_accounts

    def iterate_over_accounts(self, df, account, i):
        page = f'https://www.twitter.com/{account}'
        try:
            resp = self.api.get_user(screen_name=account)
            try:
                description = self.former[self.former['screen_name'] == account].description.iloc[0]
            except IndexError:
                description = ''
            except AttributeError:
                return df
        except tweepy.errors.NotFound:
            print(f'{i}. No account found: {page}')
            return df
        except tweepy.errors.Forbidden:
            print(f'{i}. Forbidden error, probably user has been suspended: {page}')
            return df
        return df.append(pd.DataFrame([account, resp.name, resp.friends_count, description]).T)

    @staticmethod
    def prepare_accounts_df(df):
        df.columns = ['screen_name', 'name', 'following', 'description']
        df['date'] = dp.datetime_desired_format(datetime.datetime.now())
        return df

    def show_new_followers(self):
        if self.selected_accounts.empty:
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
                  f'TwitterFollowersMonitor: No new followings for accounts.')
            return
        print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
              f'TwitterFollowersMonitor: '
              f'{self.selected_accounts.date_y.iloc[0]} - {self.selected_accounts.date_x.iloc[0]}.')
        dp.print_timedelta(self.selected_accounts)
        print('\n')
        for index, row in self.selected_accounts.iterrows():
            print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
                  f'{row["name"]} ({row["screen_name_x"]}) is following {row["diff"]} new accounts. '
                  f'{row["name"]} account description: {row["description_x"]}')
            while True:
                try:
                    obj = self.api.get_friends(screen_name=row['screen_name_x'], count=row['diff'])
                    for number in range(row['diff']):
                        print(f'https://www.twitter.com/{obj[number].screen_name}')
                    print('\n')
                    time.sleep(5)
                    break
                except tweepy.errors.TooManyRequests:
                    seconds = 960
                    print(f'{dp.datetime_desired_format(datetime.datetime.now())}. '
                          f'RateLimit - Too Many Requests. Waiting {seconds} for iterating once again.')
                    time.sleep(seconds)
                except tweepy.errors.Unauthorized:
                    print(f'{dp.datetime_desired_format(datetime.datetime.now())}. Account private')
                    break
        return


if __name__ == '__main__':
    TwitterFollowersMonitor(save_in_db=False)
