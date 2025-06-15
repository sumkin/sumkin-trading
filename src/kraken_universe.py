import sys
import json
import requests

from defs import ROOT_FOLDER
sys.path.append(ROOT_FOLDER)
from kraken_api_keys import *
from universe import Universe

class KrakenUniverse(Universe):

    def __init__(self):
        pass

    def get_tickers(self, market="futures"):
        if market == "futures":
            url = "https://futures.kraken.com/derivatives/api/v3/instruments"
            payload = {}
            headers = {
                "Accept": "application/json"
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            instruments = json.loads(response.text)["instruments"]

            res = []
            for instrument in instruments:
                symbol = instrument["symbol"]
                if "PF" in symbol and "USD" in symbol:
                    res.append(symbol)
            return res
        elif market == "spot":
            url = "https://api.kraken.com/0/public/AssetPairs"
            payload = {}
            headers = {
                "Accept": "application/json"
            }
            response = requests.request("GET", url, headers=headers, data=payload)
            pairs = json.loads(response.text)["result"]

            res = []
            for pair_name, pair_data in pairs.items():
                # Filter for USD pairs (includes USD and USDT)
                if "USD" in pair_name:
                    res.append(pair_name)

            return res
        else:
            assert False

if __name__ == "__main__":
    ku = KrakenUniverse()

    print("Futures market:")
    futures_tickers = ku.get_tickers()
    print(len(futures_tickers))
    print(futures_tickers)
    print("")
    print("Spot market:")
    spot_tickers = ku.get_tickers(market="spot")
    print(len(spot_tickers))
    print(spot_tickers)

