import sys
import pytz
import time
import pandas as pd
import json
import requests
from datetime import datetime, timedelta
sys.path.append("..")

from kraken_api_keys import KRAKEN_SPOT_PUBLIC_KEY
from data_reader import DataReader
from kraken_universe import *
from time_frame import *

class KrakenDataReader(DataReader):
    """
    Reads the data.
    """

    def __init__(self):
        pass

    def get_bars_df(self, ticker, tf, start, end, market="futures"):
        if market == "futures":
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
                "API-Key": KRAKEN_SPOT_PUBLIC_KEY
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
        elif market == "spot":
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

            # Convert timeframe to Kraken spot market interval
            # Assuming TimeFrame.get_resolution(tf) returns values like '1m', '1h', etc.
            # Kraken spot API uses integer minutes: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600
            interval_map = {
                '1m': 1, '5m': 5, '15m': 15, '30m': 30,
                '1h': 60, '4h': 240, '1d': 1440, '1w': 10080
            }

            # Get appropriate interval for spot market
            # This part depends on what your TimeFrame.get_resolution() returns
            resolution_str = TimeFrame.get_resolution(tf)
            interval = interval_map.get(resolution_str, 60)  # Default to 1h if not found

            url = f"https://api.kraken.com/0/public/OHLC?pair={ticker}&interval={interval}&since={start_ts}"

            payload = {}
            headers = {
                "Accept": "application/json"
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

            result = json.loads(response.text)["result"]

            # The first key in result should be the pair name, except for 'last' which is timestamp
            pair_data = None
            for key in result:
                if key != 'last':
                    pair_data = result[key]
                    break

            if pair_data:
                for candle in pair_data:
                    timestamp = candle[0]  # Spot API returns time in seconds
                    if timestamp > end_ts:
                        continue

                    dt = datetime.fromtimestamp(timestamp)
                    open_price = float(candle[1])
                    high = float(candle[2])
                    low = float(candle[3])
                    close = float(candle[4])
                    volume = float(candle[6])  # Note: spot API uses different index for volume

                    res_data["datetime"].append(dt)
                    res_data["open"].append(open_price)
                    res_data["high"].append(high)
                    res_data["low"].append(low)
                    res_data["close"].append(close)
                    res_data["volume"].append(volume)

            res_df = pd.DataFrame(data=res_data)
            return res_df
        else:
            assert False

    def get_price(self, ticker, tf, dt):
        return None

    def get_order_book(self, ticker, count, market="spot"):
        if market == "spot":
            url = "https://api.kraken.com/0/public/Depth"
            params = {
                "pair": ticker,
                "count": count
            }

            try:
                response = requests.get(url, params=params)
                data = response.json()

                if data["error"]:
                    print(f"API Error: {data['error']}")
                    return None

                result_key = list(data["result"].keys())[0]
                orderbook = data["result"][result_key]

                asks_df = pd.DataFrame(orderbook["asks"], columns=["price", "volume", "timestamp"])
                bids_df = pd.DataFrame(orderbook["bids"], columns=["price", "volume", "timestamp"])
                asks_df = asks_df[["price", "volume"]]
                bids_df = bids_df[["price", "volume"]]

                asks_df["price"] = asks_df["price"].astype(float)
                asks_df["volume"] = asks_df["volume"].astype(float)
                bids_df["price"] = bids_df["price"].astype(float)
                bids_df["volume"] = bids_df["volume"].astype(float)

                bids_df = bids_df.sort_values("price", ascending=False)

                return {
                    "asks": asks_df,
                    "bids": bids_df
                }
            except requests.exceptions.RequestException as e:
                print(f"Request Error: {e}")
                return None
            except (KeyError, json.JSONDecodeError) as e:
                print(f"Data Processing Error: {e}")
                return None
        elif market == "futures":
            url = f"https://futures.kraken.com/derivatives/api/v3/orderbook"
            params = {
                "symbol": ticker,
                "depth": count
            }

            try:
                response = requests.get(url, params=params)
                data = response.json()

                orderbook = data.get("orderBook", {})

                asks_df = pd.DataFrame(orderbook["asks"], columns=["price", "volume"])
                bids_df = pd.DataFrame(orderbook["bids"], columns=["price", "volume"])

                asks_df["price"] = asks_df["price"].astype(float)
                asks_df["volume"] = asks_df["volume"].astype(float)
                bids_df["price"] = bids_df["price"].astype(float)
                bids_df["volume"] = bids_df["volume"].astype(float)

                bids_df = bids_df.sort_values("price", ascending=False)

                return {
                    "asks": asks_df,
                    "bids": bids_df
                }
            except requests.exceptions.RequestException as e:
                print(f"Request Error: {e}")
                return None
            except (KeyError, json.JSONDecodeError) as e:
                print(f"Data Processing Error: {e}")
                return None
        else:
            assert False

    def get_best_bid(self, ticker, market="spot"):
        res = self.get_order_book(ticker, 10, market=market)
        bids = res["bids"]
        if bids.shape[0] == 0:
            return None, None
        return bids.iloc[0]

    def get_best_ask(self, ticker, market="spot"):
        res = self.get_order_book(ticker, 10, market=market)
        asks = res["asks"]
        if asks.shape[0] == 0:
            return None, None
        return asks.iloc[0]

if __name__ == "__main__":
    tz = pytz.timezone("UTC")
    start = datetime.now() - timedelta(days=365)
    start = tz.localize(start)

    ku = KrakenUniverse()
    kdr = KrakenDataReader()
    tickers = ku.get_tickers(market="spot")
    for ticker in tickers:
        price, amnt = kdr.get_best_bid(ticker, market="spot")
        print(ticker, price, amnt)