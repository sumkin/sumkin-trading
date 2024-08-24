import os
import pytz
import numpy as np
import pandas as pd
import pickle
from mpire import WorkerPool
from datetime import datetime, timedelta
import statsmodels.api as sm
import statsmodels.tsa.stattools as ts

from time_frame import TimeFrame
from tinkoff_universe import TinkoffUniverse
from tinkoff_data_reader import TinkoffDataReader

class CointegrationPairsFinder:

    def __init__(self, tf: TimeFrame, start: datetime, end: datetime):
        self.tf = tf
        self.start = start
        self.end = end

    def _read_dfs(self):
        tu = TinkoffUniverse()
        shares = tu.get_shares()

        self.dfs = {}
        pickle_fname = "../cache/cointegration_pairs_finder/dfs.pickle"
        if os.path.isfile(pickle_fname):
            with open(pickle_fname, "rb") as handle:
                self.dfs = pickle.load(handle)
        else:
            parallel = False
            if parallel:
                with WorkerPool(n_jobs=os.cpu_count()) as pool:
                    tdr = TinkoffDataReader()
                    f = lambda figi: tdr.get_bars_df(figi, self.tf, start, end)
                    dfs = pool.map(f, [share[1] for share in shares])
                    assert len(shares) == len(dfs)
                    self.dfs = {}
                    for i in range(len(shares)):
                        self.dfs[shares[i][0]] = dfs[i]
                    with open(pickle_fname, "wb") as handle:
                        pickle.dump(self.dfs, handle)
            else:
                for i, share in enumerate(shares):
                    tdr = TinkoffDataReader()
                    df = tdr.get_bars_df(share[1], self.tf, start, end)
                    self.dfs[share[0]] = df
                    print(i, len(shares), share[0], df.shape[0])
                with open(pickle_fname, "wb") as handle:
                    pickle.dump(self.dfs, handle)

    def _filter_by_num_candles(self):
        """
        Delete 10% of tickers with the lowest number of candles.
        """
        num_candles_arr = np.array([self.dfs[ticker].shape[0] for ticker in self.dfs.keys()])
        q = np.quantile(num_candles_arr, 0.1)
        tickers = self.dfs.keys()

        to_remove = []
        for ticker in tickers:
            if self.dfs[ticker].shape[0] < q:
                to_remove.append(ticker)
        for ticker in to_remove:
            del self.dfs[ticker]

    def _filter_by_volume(self):
        """
        Delete 10% of tickers with the lowest volume.
        """
        volumes_arr = np.array([self.dfs[ticker]["volume"].mean() for ticker in self.dfs.keys()])
        q = np.quantile(volumes_arr, 0.1)
        tickers = self.dfs.keys()

        to_remove = []
        for ticker in tickers:
            if self.dfs[ticker]["volume"].mean() < q:
                to_remove.append(ticker)
        for ticker in to_remove:
            del self.dfs[ticker]

    def _find_pairs(self):
        self.pairs = []
        tickers = list(self.dfs.keys())
        for i in range(len(tickers)):
            t1 = tickers[i]
            for j in range(i + 1, len(tickers)):
                t2 = tickers[j]
                df = pd.merge(self.dfs[t1], self.dfs[t2], on="datetime").dropna()
                vals1 = np.array(df["close_x"].values)
                vals2 = np.array(df["close_y"].values)
                ols_result = sm.OLS(vals2, vals1).fit()
                p_val = ts.adfuller(ols_result.resid)[1]
                if p_val < 0.01:
                    self.pairs.append([t1, t2])

    def get_num_tickers(self):
        return len(self.dfs.keys())

    def get_num_pairs(self):
        return len(self.pairs)

if __name__ == "__main__":
    tz = pytz.timezone("UTC")
    end = datetime.now()
    start = datetime.now() - timedelta(days=365)
    start = tz.localize(start)
    end = tz.localize(end)

    cpf = CointegrationPairsFinder(TimeFrame.INTERVAL_DAY, start, end)
    cpf._read_dfs()
    cpf._filter_by_num_candles()
    cpf._filter_by_volume()
    cpf._find_pairs()
    num_tickers = cpf.get_num_tickers()
    num_pairs = cpf.get_num_pairs()
    print("Number of tickers = {}".format(num_tickers))
    print("Number of pairs = {}".format(num_pairs))