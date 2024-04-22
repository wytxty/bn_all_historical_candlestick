"""
Get all historical kline data of Binance

type: str(), 's' or 'pu', s means spot data, pu means usdm data
symbol: str()
start_time: 0 means from the beginning of data
interval: str()
limit_per_batch: int(), size of small batch you wanna run

https://binance-docs.github.io/apidocs/spot/en/#kline-candlestick-data
https://binance-docs.github.io/apidocs/futures/en/#change-log

"""

author = "wytxty"

import requests
import time
import pandas as pd
from datetime import datetime


class get_bn_all_klines():

    def __init__(self, proxies):

        self.proxies = proxies
        self.LABELS = ['kline_open_time', 'open', 'high', 'low', 'close', 'volume',
                       'kline_close_time', 'quote_volume', 'trades_num',
                       'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore']

    def get_batch(self, type, symbol, start_time, interval, limit_per_batch):

        if type == 's':
            API_BASE = 'https://api.binance.com/api/v3/'
        if type == 'pu':
            API_BASE = 'https://fapi.binance.com/fapi/v1/'

        params = {'symbol': symbol, 'interval': interval, 'startTime': start_time, 'limit': limit_per_batch}

        try:
            response = requests.get(url=f'{API_BASE}klines', params=params, timeout=30, proxies=self.proxies)
        except requests.exceptions.ConnectionError:
            print('Connection error, Cooling down for 10 seconds')
            time.sleep(10)
            return self.get_batch(type, symbol, start_time, interval, limit_per_batch)

        except requests.exceptions.Timeout:
            print('Timeout, Cooling down for 10 seconds')
            time.sleep(10)
            return self.get_batch(type, symbol, start_time, interval, limit_per_batch)

        except requests.exceptions.ConnectionResetError:
            print('Connection reset error, Cooling down for 10 seconds')
            time.sleep(10)
            return self.get_batch(type, symbol, start_time, interval, limit_per_batch)

        if response.status_code == 200:
            return pd.DataFrame(response.json(), columns=self.LABELS)
        print(f'Got erroneous response back: {response}')
        return pd.DataFrame([])

    def get_all_batch(self, type, symbol, start_time, interval, limit_per_batch):

        batches = [pd.DataFrame([], columns=self.LABELS)]

        last_timestamp = 0
        while last_timestamp / 1000 <= int(time.time()):

            new_batch = self.get_batch(type=type, symbol=symbol, start_time=start_time, interval=interval,
                                       limit_per_batch=limit_per_batch)

            start_time = new_batch['kline_open_time'].max()
            last_timestamp = new_batch['kline_close_time'].max()
            print(datetime.utcfromtimestamp(last_timestamp / 1000))

            batches.append(new_batch)

        df = pd.concat(batches, ignore_index=True)
        df = df.reset_index(drop=True)
        df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        df = df.reset_index(drop=True)
        df = df.rename(columns={'quote_asset_volume': 'quote_volume'})
        df["kline_open_time"] = pd.to_datetime(df["kline_open_time"], unit="ms", utc=True)

        delta = df["kline_open_time"].iloc[1] - df["kline_open_time"].iloc[0]
        df["time"] = df["kline_open_time"] + delta
        df = df[['time', 'open', 'high', 'low', 'close', 'volume', 'quote_volume']].copy()

        return df

if __name__ == "__main__":
    proxy_host = "127.0.0.1"
    proxy_port = 0
    proxies = {}
    if proxy_host and proxy_port:
        proxy = f'http://{proxy_host}:{proxy_port}'
        proxies = {'http': proxy, 'https': proxy}
    k = get_bn_all_klines(proxies=proxies)
    df = k.get_all_batch(type='pu', symbol='BTCUSDT', start_time=0, interval='1m', limit_per_batch=500)

