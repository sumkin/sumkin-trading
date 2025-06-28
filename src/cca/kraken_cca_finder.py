from kraken_universe import KrakenUniverse
from kraken_data_reader import KrakenDataReader

KRAKEN_COMMISSION=0.30

class KrakenCCAFinder:

    def __init__(self):
        pass

    def get_tickers(self):
        ku = KrakenUniverse()

        # Spot market tickers.
        spot_tickers = ku.get_tickers(market="spot")

        # Futures market tickers.
        futures_tickers = ku.get_tickers(market="futures")

        tickers = []
        for spot_ticker in spot_tickers:
            for futures_ticker in futures_tickers:
                if spot_ticker == futures_ticker[3:]:
                    tickers.append([spot_ticker, futures_ticker])

        return tickers

    def find(self):
        kdr = KrakenDataReader()

        tickers = self.get_tickers()
        for ticker in tickers:
            bid_price, bid_vol,  = kdr.get_best_bid(ticker[0], market="spot")
            if bid_price is None:
                continue
            ask_price, ask_vol = kdr.get_best_ask(ticker[1], market="futures")
            if ask_price is None:
                continue
            if bid_price > ask_price:
                vol = min(bid_vol, ask_vol)
                deal = bid_price * vol
                profit = (bid_price - ask_price) * vol
                if (100 * profit / deal) > KRAKEN_COMMISSION:
                    yield ticker[0], ticker[1], bid_price, ask_price, vol



if __name__ == "__main__":
    kccaf = KrakenCCAFinder()
    kccaf.find()
