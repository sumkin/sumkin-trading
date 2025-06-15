import pytz
import pandas as pd
import scipy.stats as stats
from datetime import datetime, timedelta
import matplotlib.pyplot as plt

from time_frame import *
from kraken_universe import KrakenUniverse
from kraken_data_reader import KrakenDataReader

class KrakenCashAndCarryArbitrageFinder:

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
            bid_price, bid_num, _ = kdr.get_best_bid(ticker[0], market="spot")
            if bid_price is None:
                continue
            ask_price, ask_num = kdr.get_best_ask(ticker[1], market="futures")
            if ask_price is None:
                continue
            bid_price = float(bid_price)
            ask_price = float(ask_price)
            bid_num = float(bid_num)
            ask_num = float(ask_num)
            if bid_price > ask_price:
                vol = min(bid_num, ask_num)
                deal = bid_price * vol
                profit = (bid_price - ask_price) * vol
                print(ticker)
                print("\t", bid_price, ask_price, vol, deal, profit, round(100 * profit / deal, 3), "%")
                print("")

if __name__ == "__main__":
    kccaf = KrakenCashAndCarryArbitrageFinder()
    kccaf.find()
