from tools import data_preparation as dp
import json
import pandas as pd
import datetime
import warnings
warnings.filterwarnings("ignore")


def prepare_unlocking_tokens_info(row):
    symbol = get_token_symbol(row)
    name = get_token_name(row)
    price = get_price(row)
    total_supply = get_total_supply(row)
    circ_supply = get_circ_supply(row)
    locked_perc = get_locked_perc(row)
    fdv = get_fdv(row)
    mcap = get_mcap(row)
    amount = get_amount(row)
    unlock_value = get_unlock_value(amount, price)
    unlock_perc = get_unlock_perc(amount, total_supply)
    begin_date = get_begin_date(row)
    return pd.DataFrame([symbol, name, price, total_supply, circ_supply, locked_perc,
                         fdv, mcap, unlock_value, unlock_perc, begin_date]).T


def prepare_unlocks_df():
    print(f"{dp.datetime_desired_format(datetime.datetime.now())}. Unlocks report started.")
    data = dp.get_json('https://token.unlocks.app/?category=all', False)
    json_string = data.split("}]}},")[1].split('"tokenCategory"')[0][7:-2]
    js = json.loads(json_string)
    d = pd.DataFrame(js)
    df = pd.DataFrame()
    for index, row in d.iterrows():
        info = prepare_unlocking_tokens_info(row)
        df = pd.concat([df, info])
    df.columns = ['symbol', 'name', 'price', 'total_supply', 'circ_supply',
                  'locked_perc', 'fdv', 'mcap', 'unlock_value', 'unlock_perc', 'begin_date']
    print(f"{dp.datetime_desired_format(datetime.datetime.now())}. Unlocks report finished.")
    return df.sort_values('begin_date').reset_index(drop=True)


def get_unlock_value(amount, price):
    try:
        return round(amount*price, 0)
    except IndexError:
        return 0
    except ValueError:
        return 0
    except TypeError:
        return 0


def get_unlock_perc(amount, total_supply):
    try:
        return round(amount/total_supply, 3)
    except IndexError:
        return 0
    except ValueError:
        return 0
    except ZeroDivisionError:
        return 0
    except TypeError:
        return 0


def get_amount(row):
    try:
        return float(row['nextEventData']['amount'])
    except IndexError:
        return 0
    except ValueError:
        return 0
    except ZeroDivisionError:
        return 0
    except TypeError:
        return 0


def get_token_symbol(row):
    try:
        return row['token']['symbol']
    except IndexError:
        return None
    except TypeError:
        return None


def get_token_name(row):
    try:
        return row['token']['name']
    except IndexError:
        return None
    except TypeError:
        return None


def get_price(row):
    try:
        return float(row['token']['price'])
    except IndexError:
        return None
    except TypeError:
        return None


def get_total_supply(row):
    try:
        return int(float(row['token']['maxSupply']))
    except IndexError:
        return None
    except ValueError:
        return None
    except TypeError:
        return None


def get_circ_supply(row):
    try:
        return int(float(row['token']['circulatingSupply']))
    except IndexError:
        return None
    except ValueError:
        return None
    except TypeError:
        return None


def get_locked_perc(row):
    try:
        return float(row['totalLockedPercent'])
    except IndexError:
        return None
    except ValueError:
        return None
    except TypeError:
        return None


def get_fdv(row):
    try:
        return int(float(row['token']['fullyDiluted']))
    except IndexError:
        return None
    except ValueError:
        return None
    except TypeError:
        return None


def get_mcap(row):
    try:
        return int(float(row['token']['marketCap']))
    except IndexError:
        return None
    except ValueError:
        return None
    except TypeError:
        return None


def get_begin_date(row):
    try:
        dt = row['nextEventData']['beginDate']
        string_date = dt.replace('T', ' ').replace('Z', '')
        return datetime.datetime.strptime(string_date[:19], '%Y-%m-%d %H:%M:%S')
    except IndexError:
        return None
    except ValueError:
        return None
    except ZeroDivisionError:
        return None
    except TypeError:
        return None


def open_unlocks_website():
    return dp.open_url_in_jupyter('https://token.unlocks.app/')


if __name__ == '__main__':
    df = prepare_unlocks_df()
    print(df.head(10))
