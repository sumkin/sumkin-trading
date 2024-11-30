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
from cointegration_pair_checker import CointegrationPairChecker

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
        if False: #os.path.isfile(pickle_fname):
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

                # Split dataframe in-sample (is) and out-of-sample (os).
                df_is = df[:int(df.shape[0] * 0.5)]
                df_os = df[int(df.shape[0]  * 0.5):]

                cpc = CointegrationPairChecker(df_is)
                res, hedge_ratio, intercept, resid = cpc.cointegrate()
                if not res:
                    continue

                df_os["resid"] = df_os["close2"] - hedge_ratio * df_os["close1"] - intercept
                resid_std = np.std(resid)

                chart1_is = go.Scatter(
                    x=df_is["datetime"],
                    y=df_is["close1"],
                    mode="lines",
                    name=t1,
                    marker=dict(color="orangered")
                )
                chart1_os = go.Scatter(
                    x=df_os["datetime"],
                    y=df_os["close1"],
                    mode="lines",
                    name=t1,
                    marker=dict(color="orangered")
                )
                chart2_is = go.Scatter(
                    x=df_is["datetime"],
                    y=df_is["close2"],
                    mode="lines",
                    name=t2,
                    marker=dict(color="darkred")
                )
                chart2_os = go.Scatter(
                    x=df_os["datetime"],
                    y=df_os["close2"],
                    mode="lines",
                    name=t2,
                    marker=dict(color="darkred")
                )
                volume1_is = go.Scatter(
                    x=df_is["datetime"],
                    y=df_is["volume1"],
                    mode="lines",
                    name="{} volume".format(t1),
                    marker=dict(color="lightgreen")
                )
                volume1_os = go.Scatter(
                    x=df_os["datetime"],
                    y=df_os["volume1"],
                    mode="lines",
                    name=t1,
                    marker=dict(color="lightgreen")
                )
                volume2_is = go.Scatter(
                    x=df_is["datetime"],
                    y=df_is["volume2"],
                    mode="lines",
                    name="{} volume".format(t2),
                    marker=dict(color="darkgreen")
                )
                volume2_os = go.Scatter(
                    x=df_os["datetime"],
                    y=df_os["volume2"],
                    mode="lines",
                    name="{} volume".format(t2),
                    marker=dict(color="darkgreen")
                )
                regres_is = go.Scatter(
                    x=df_is["close1"],
                    y=df_is["close2"],
                    mode="markers",
                    marker=dict(color="orange"),
                    name="{}-{} prices".format(t1, t2)
                )
                regres_os = go.Scatter(
                    x=df_os["close1"],
                    y=df_os["close2"],
                    mode="markers",
                    marker=dict(color="orange"),
                    name="{}-{} prices".format(t1, t2)
                )
                resid_min = min(min(resid), min(df_os["resid"]))
                resid_max = max(max(resid), max(df_os["resid"]))
                regres_min = min(min(df_is["close1"].min(), df_is["close2"].min()), min(df_os["close1"].min(), df_os["close2"].min()))
                regres_max = max(max(df_is["close1"].max(), df_is["close2"].max()), max(df_os["close1"].max(), df_os["close2"].max()))
                resid_is = go.Scatter(
                    x=df_is["datetime"],
                    y=resid,
                    mode="lines",
                    name="residuals",
                    marker=dict(color="blue")
                )
                resid_os = go.Scatter(
                    x=df_os["datetime"],
                    y=df_os["resid"],
                    mode="lines",
                    name="residuals",
                    marker=dict(color="blue")
                )
                fname = "../output/cointegration_pairs_finder/{}_{}.html".format(t1, t2)
                fig = make_subplots(rows=4, cols=2)
                fig.update_yaxes(range=[resid_min, resid_max], row=4, col=1)
                fig.update_yaxes(range=[resid_min, resid_max], row=4, col=2)
                fig.update_xaxes(range=[regres_min, regres_max], row=3, col=1)
                fig.update_yaxes(range=[regres_min, regres_max], scaleanchor="x", scaleratio=1, row=3, col=1)
                fig.update_xaxes(range=[regres_min, regres_max], row=3, col=2)
                fig.update_yaxes(range=[regres_min, regres_max], scaleanchor="x", scaleratio=1, row=3, col=2)
                fig.add_trace(chart1_is, 1, 1)
                fig.add_trace(chart2_is, 1, 1)
                fig.add_trace(chart1_os, 1, 2)
                fig.add_trace(chart2_os, 1, 2)
                fig.add_trace(volume1_is, 2, 1)
                fig.add_trace(volume2_is, 2, 1)
                fig.add_trace(volume1_os, 2, 2)
                fig.add_trace(volume2_os, 2, 2)
                fig.add_trace(regres_is, 3, 1)
                fig.add_trace(regres_os, 3, 2)
                fig.add_trace(resid_is, 4, 1)
                fig.add_hline(y=0.75 * resid_std, row=4, col=1, line_dash="dash", line_color="red", line_width=1)
                fig.add_hline(y=-0.75 * resid_std, row=4, col=1, line_dash="dash", line_color="red", line_width=1)
                fig.add_trace(resid_os, 4, 2)
                fig.add_hline(y=0.75 * resid_std, row=4, col=2, line_dash="dash", line_color="red", line_width=1)
                fig.add_hline(y=-0.75 * resid_std, row=4, col=2, line_dash="dash", line_color="red", line_width=1)
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
    start = end - timedelta(days=150)
    start = tz.localize(start)
    end = tz.localize(end)

    cpf = CointegrationPairsFinder(TimeFrame.INTERVAL_DAY, start, end)
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