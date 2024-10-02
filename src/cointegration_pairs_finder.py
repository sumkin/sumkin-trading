import os
import pytz
import numpy as np
import pandas as pd
import pickle
from mpire import WorkerPool
from datetime import datetime, timedelta
import statsmodels.formula.api as smf
import statsmodels.stats.api as sms
import statsmodels.tsa.stattools as ts
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from scipy.stats import ttest_1samp

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
                    if df.shape[0] == 0:
                        continue
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
        self.pairs_info = []
        tickers = list(self.dfs.keys())
        for i in range(len(tickers)):
            t1 = tickers[i]
            for j in range(i + 1, len(tickers)):
                t2 = tickers[j]
                df = pd.merge(self.dfs[t1], self.dfs[t2], on="datetime", suffixes=("1", "2")).dropna()
                df = df[["datetime", "volume1", "volume2", "close1", "close2"]]
                fit = smf.ols("close2 ~ close1", data=df).fit()

                # Check volume in money. It should not be less than 1 million for both stocks.
                money_volume1 = np.mean(df["volume1"] * df["close1"])
                money_volume2 = np.mean(df["volume2"] * df["close2"])
                if money_volume1 < 10e6 or money_volume2 < 10e6:
                    continue

                # Prices should not differ more than 10 times.
                mean_price1 = df["close1"].mean()
                mean_price2 = df["close2"].mean()
                mean_price_ratio = mean_price2 / mean_price1
                if mean_price_ratio < 0.1 or mean_price_ratio > 10:
                    continue

                # Limit hedge ratio.
                hedge_ratio = fit.params["close1"]
                if abs(hedge_ratio) > 5 or abs(hedge_ratio) < 0.2:
                    continue

                # Test for stationarity of residuals.
                p_val_adfuller = ts.adfuller(fit.resid)[1]
                if p_val_adfuller > 0.005:
                    continue

                # Test for homoscedasticity.
                p_val_bp = sms.het_breuschpagan(fit.resid, fit.model.exog)[1]
                if p_val_bp < 0.02:
                    continue

                # Test for zero mean.
                p_val_zm = ttest_1samp(fit.resid, 0.0).pvalue
                if p_val_zm < 0.01:
                    continue

                resid_std = np.std(fit.resid)

                chart1 = go.Scatter(
                    x=df["datetime"],
                    y=df["close1"],
                    mode="lines",
                    name=t1
                )
                chart2 = go.Scatter(
                    x=df["datetime"],
                    y=df["close2"],
                    mode="lines",
                    name=t2
                )
                volume1 = go.Scatter(
                    x=df["datetime"],
                    y=df["volume1"],
                    mode="lines",
                    name="{} volume".format(t1)
                )
                volume2 = go.Scatter(
                    x=df["datetime"],
                    y=df["volume2"],
                    mode="lines",
                    name="{} volume".format(t2)
                )
                regres = go.Scatter(
                    x=df["close1"],
                    y=df["close2"],
                    mode="markers",
                    name="{}-{} prices".format(t1, t2)
                )
                resid = go.Scatter(
                    x=df["datetime"],
                    y=fit.resid,
                    mode="lines",
                    name="residuals"
                )
                fname = "../output/cointegration_pairs_finder/{}_{}.html".format(t1, t2)
                fig = make_subplots(rows=4, cols=1)
                fig.add_trace(chart1, 1, 1)
                fig.add_trace(chart2, 1, 1)
                fig.add_trace(volume1, 2, 1)
                fig.add_trace(volume2, 2, 1)
                fig.add_trace(regres, 3, 1)
                fig.add_trace(resid, 4, 1)
                fig.add_hline(y=resid_std, row=4, col=1, line_dash="dash", line_color="red", line_width=1)
                fig.add_hline(y=-resid_std, row=4, col=1, line_dash="dash", line_color="red", line_width=1)
                fig.write_html(fname)
                self.pairs.append([t1, t2])
                self.pairs_info.append({"std": resid_std})
                print("\t", t1, t2, resid_std)

    def get_num_tickers(self):
        return len(self.dfs.keys())

    def get_num_pairs(self):
        return len(self.pairs)

if __name__ == "__main__":
    tz = pytz.timezone("UTC")
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=10)
    start = tz.localize(start)
    end = tz.localize(end)

    cpf = CointegrationPairsFinder(TimeFrame.INTERVAL_HOUR, start, end)
    print("Reading dataframes...")
    cpf._read_dfs()
    print("Filtering by number of candles...")
    cpf._filter_by_num_candles()
    print("Filtering by volume...")
    cpf._filter_by_volume()
    print("Finding pairs...")
    cpf._find_pairs()
    num_tickers = cpf.get_num_tickers()
    num_pairs = cpf.get_num_pairs()
    print("Number of tickers = {}".format(num_tickers))
    print("Number of pairs = {}".format(num_pairs))