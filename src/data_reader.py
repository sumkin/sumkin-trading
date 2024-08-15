import os
import sys
import pandas as pd
from alpaca.data.timeframe import TimeFrame
from alpaca.data.requests import StockBarsRequest
from alpaca.data.historical import StockHistoricalDataClient
sys.path.append("..")

from alpaca_keys import *

class DataReader:
    """
    Reads the data.
    """

    def __init__(self):
        self.client = StockHistoricalDataClient(API_KEY, SECRET_KEY)

    def read_bars(self, ticker, timeframe, start, end):
        """
        Reads bars data.
        :param symbol: e.g. 'TSLA'
        :param timeframe: TimeFrameUnit.Minute
        :param start: 'YYYY-MM-DD hh:mm:ss'
        :param end: 'YYYY-MM-DD hh:mm:ss'
        :return: pandas Dataframe
        """

        # Check cache.
        fname = "../data/{}_{}_{}_{}.csv.gz".format(ticker,
                                                    start.replace(":", "").replace(" ", "_"),
                                                    end.replace(":", "").replace(" ", "_"),
                                                    str(timeframe))
        if os.path.isfile(fname):
            res_df = pd.read_csv(fname)
            res_df["Time"] = pd.to_datetime(res_df["Time"])
            res_df = res_df.set_index("Time")
        else:
            request_params = StockBarsRequest(symbol_or_symbols=[ticker],
                                              timeframe=timeframe,
                                              start=start,
                                              end=end)
            bars = self.client.get_stock_bars(request_params)
            res_df = bars.df.reset_index()
            res_df = res_df.rename(columns={"timestamp": "Time",
                                            "open": "Open",
                                            "high": "High",
                                            "low": "Low",
                                            "close": "Close",
                                            "volume": "Volume"})
            res_df["Time"] = pd.to_datetime(res_df["Time"])
            res_df = res_df.set_index("Time")
            res_df.to_csv(fname)
        return res_df
