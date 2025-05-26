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

    def get_tickers(self):
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

if __name__ == "__main__":
    ku = KrakenUniverse()
    tickers = ku.get_tickers()
    print(len(tickers))
    print(tickers)

