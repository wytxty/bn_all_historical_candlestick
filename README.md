Request packages including requests, time, pandas, datetime

You can get all historical kline data of Binance
* Notice, the time in the final df is represents the kline close time, not the kline open time.

Example:

    proxy_host = "127.0.0.1"
    proxy_port = 0
    proxies = {}
    if proxy_host and proxy_port:
        proxy = f'http://{proxy_host}:{proxy_port}'
        proxies = {'http': proxy, 'https': proxy}
    k = get_bn_all_klines(proxies=proxies)
    df = k.get_all_batch(type='pu', symbol='BTCUSDT', start_time=0, interval='1m', limit_per_batch=500)
