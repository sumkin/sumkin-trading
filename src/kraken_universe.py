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
        url = "https://api.kraken.com/0/public/AssetPairs"
        payload = {}
        headers = {
            "Accept": "application/json"
        }
        response = requests.request("GET", url, headers=headers, data=payload)
        result = json.loads(response.text)["result"]

        res = []
        for k in result.keys():
            altname = result[k]["altname"]
            if altname[-3:] == "USD":
                res.append(altname)
        return res

if __name__ == "__main__":
    ku = KrakenUniverse()
    tickers = ku.get_tickers()
    print(tickers)

