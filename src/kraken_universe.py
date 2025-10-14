import sys
import ccxt

from defs import ROOT_FOLDER
sys.path.append(ROOT_FOLDER)
from kraken_api_keys import *
from universe import Universe

class KrakenUniverse(Universe):

    def __init__(self):
        self.spot_exchange = ccxt.kraken({
            "apiKey": KRAKEN_SPOT_PUBLIC_KEY,
            "secret": KRAKEN_SPOT_PRIVATE_KEY
        })
        self.futures_exchange = ccxt.krakenfutures({
            "apiKey": KRAKEN_FUTURES_PUBLIC_KEY,
            "secret": KRAKEN_FUTURES_PRIVATE_KEY
        })

    def get_tickers(self, market="futures"):
        if market == "futures":
            markets = self.futures_exchange.load_markets()
            res = []
            for symbol in markets.keys():
                if "PF" in markets[symbol]["id"] and "USD" == markets[symbol]["id"].strip()[-3:]:
                    res.append(markets[symbol]["id"])
            return res
        elif market == "spot":
            markets = self.spot_exchange.load_markets()
            res = []
            for symbol in markets.keys():
                if "USD" == markets[symbol]["id"].strip()[-3:]:
                    res.append(markets[symbol]["id"])
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

