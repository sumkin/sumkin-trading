import sys
import pytz
import time
import json
import requests
import pandas as pd
from datetime import datetime, timedelta
sys.path.append("..")

from kraken_api_keys import KRAKEN_API_KEY
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

        resolution = TimeFrame.get_resolution(tf)
        url = "https://futures.kraken.com/api/charts/v1/trade/{}/{}?from={}".format(ticker, resolution, start_ts)

        payload = {}
        headers = {
            "Accept": "application/json",
            "API-Key": KRAKEN_API_KEY
        }
        response = None
        for i in range(15):
            try:
                response = requests.request("GET", url, headers=headers, data=payload, timeout=5)
                if response.status_code == 200:
                    break
            except:
                pass
            time.sleep(3)
        assert response is not None
        data = json.loads(response.text)["candles"]
        for d in data:
            d["time"] = d["time"] / 1000 # returns milliseconds.
            if d["time"] > end_ts:
                continue
            dt = datetime.fromtimestamp(d["time"])
            open = float(d["open"])
            high = float(d["high"])
            low = float(d["low"])
            close = float(d["close"])
            volume = float(d["volume"])
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
