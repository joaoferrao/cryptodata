import pandas as pd
import requests
import time
from datetime import datetime, timedelta
from typing import List


# Define which symbols you want to download data from
CRYPTO: List = ['BTC', 'ETH', 'EOS', 'LTC']


# Check if symbol is available
def check_if_symbol_available(symbol: str) -> bool:
    r = requests.get("https://www.cryptocompare.com/api/data/coinlist/")
    data = r.json()["Data"]
    if symbol in data.keys():
        return True
    else:
        return False


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


def query_all(symbol: str, currency: str, todate: int=None):
    url = define_rest_url(symbol=symbol, currency=currency, todate=todate)
    r, next_todate = get_batch_response(url)
    df = response_to_dataframe(r)
    while next_todate != 0:
        url = define_rest_url(symbol=symbol, currency=currency, todate=next_todate)
        r, next_todate = get_batch_response(url)
        df = df.append(response_to_dataframe(r), ignore_index=True)

    df = convert_col_int_to_dt(df, ["time"])
    df.sort_values(by=["time"])

    return df

