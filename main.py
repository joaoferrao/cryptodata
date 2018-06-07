import pandas as pd
import logging
import requests
import time
from datetime import datetime, timedelta
from typing import List


# Setup which symbols you want to download data from
CRYPTOS: List = ['BTC', 'ETH', 'EOS', 'LTC']


def define_rest_url(symbol: str, currency: str, todate: int) -> str:
    base_url = "https://min-api.cryptocompare.com/data/histohour?limit=2000"
    base_url += "&fsym={}&tsym={}".format(symbol, currency)
    if todate:
        base_url += "&toTs={}".format(todate)
    return base_url


def _get_next_date_range(resp: requests.models.Response) -> int:
    # fetch the unix time from the first entry in the response Data
    timestamp = resp.json()["Data"][0]["time"]
    timestamp = datetime.fromtimestamp(timestamp) - timedelta(hours=1)
    unixtime = int(time.mktime(timestamp.timetuple()))
    return unixtime


def get_batch_response(url: str) -> (requests.models.Response, int):
    r = requests.get(url)
    next_todate = 0
    if r.json()["Data"][0]['close'] != 0:
        next_todate = _get_next_date_range(resp=r)
    return r, next_todate


def response_to_dataframe(resp: requests.models.Response) -> pd.DataFrame:
    df = pd.DataFrame(resp.json()["Data"])
    return df


def convert_col_int_to_dt(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for col in cols:
        df[col] = pd.to_datetime(df[col], unit='s')
    return df


def query_symbol(symbol: str, currency: str, todate: int=None):
    print("Querying for {}".format(symbol))
    url = define_rest_url(symbol=symbol, currency=currency, todate=todate)
    r, next_todate = get_batch_response(url)
    df = response_to_dataframe(r)
    while next_todate != 0:
        url = define_rest_url(symbol=symbol, currency=currency, todate=next_todate)
        r, next_todate = get_batch_response(url)
        df = df.append(response_to_dataframe(r), ignore_index=True)

    df = convert_col_int_to_dt(df, ["time"])
    df.sort_values(by=["time"], inplace=True)
    df['Symbol'] = symbol
    return df


def download_data(list_cryptos: List[str], currency: str, todate: int=None):
    if len(list_cryptos) == 0:
        raise Exception

    df = pd.DataFrame()
    for symbol in list_cryptos:
        df = df.append(query_symbol(symbol, currency, todate), ignore_index=True)

    df.to_csv(path_or_buf='csv/data.csv', encoding='utf-8', index=False,)


if __name__ == '__main__':
    print("Starting Download")
    download_data(CRYPTOS, currency='USD')
