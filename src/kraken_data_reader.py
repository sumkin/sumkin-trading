import sys
import pytz
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
sys.path.append("..")

from data_reader import DataReader
from kraken_universe import *
from time_frame import *

class KrakenDataReader(DataReader):
    """
    Reads the data.
    """

    def __init__(self):
        pass

    def get_bars_df(self, ticker: str, tf: TimeFrame, start: datetime, end: datetime = None):
        start_ts = int(start.timestamp())
        if end is None:
            end_ts = int(datetime.now().timestamp())
        else:
            end_ts = int(end.timestamp())

        res_data = {
            "datetime": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": []
        }

        interval = TimeFrame.get_num_minutes(tf)
        url = "https://api.kraken.com/0/public/OHLC?pair={}&interval={}&since={}".format(ticker, interval, start_ts)
        payload = {}
        headers = {
            "Accept": "application/json"
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        data = json.loads(response.text)["result"][ticker]
        for d in data:
            if d[0] > end_ts:
                continue
            dt = datetime.fromtimestamp(d[0])
            open = d[1]
            high = d[2]
            low = d[3]
            close = d[4]
            volume = d[6]
            res_data["datetime"].append(dt)
            res_data["open"].append(open)
            res_data["high"].append(high)
            res_data["low"].append(low)
            res_data["close"].append(close)
            res_data["volume"].append(volume)

        res_df = pd.DataFrame(data=res_data)
        return res_df

    def get_price(self, ticker: str, tf: TimeFrame, dt: datetime):
        return None

if __name__ == "__main__":
    tz = pytz.timezone("UTC")
    start = datetime.now() - timedelta(days=365)
    start = tz.localize(start)

    ku = KrakenUniverse()
    tickers = ku.get_tickers()
    for ticker in tickers:
        print(ticker)
        kdr = KrakenDataReader()
        df = kdr.get_bars_df(ticker, TimeFrame.INTERVAL_HOUR, start)
        print(df.head(5))
        print(df.tail(5))
