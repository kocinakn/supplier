def binance_spot_api():
    api_key = 'kRIUPoybQXo0OJcuqx3UGVUBuk9ulS8ZyFlwyhvmsP28vW7Mlxs52qJd2qf0Yqbx'
    api_secret = 'e0rmU64PNVLpWF5IqMw7y1qp5Uyh4kcOZ2bhLl4IfZIR7mWRxttGUfXGFnqhvhjD'
    return api_key, api_secret


def binance_futures_api():
    api_key = 'OAbAiWSPA6Ekk0p6FryIZpLSNSD6iXMwfBqBWAzn478TVVZHlDmN9el1F2IntOOd'
    api_secret = 'tQZhfQdsQMlyCVsf6pfGBMLI6WXA9Y7vhCkB8sXNOiuYgFXhcTtqkjdZHWhggE0e'
    return api_key, api_secret


def kucoin_spot_api():
    api_key = '61ead35055919600012f0b9b'
    api_secret = '61d8a249-f442-4c7b-afe8-585055dc10d8'
    passphrase = 'market123'
    return api_key, api_secret, passphrase


def postgres_credentials():
    return 'postgres', '123'


def debank_api_key():
    api_key = '81e4562c1335f21b4fb73ff1793c22f9984d8d31'
    headers = {
        'accept': 'application/json',
        'AccessKey': f"{api_key}"
    }
    return headers