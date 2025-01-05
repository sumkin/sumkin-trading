import os
import pytz
import numpy as np
import pandas as pd
import pickle
from mpire import WorkerPool
from datetime import datetime, timedelta
from plotly.subplots import make_subplots
import plotly.graph_objs as go
from jinja2 import Template

from defs import ROOT_FOLDER
from time_frame import TimeFrame
from universe import Universe
from data_reader import DataReader
from cointegration_pair_checker import CointegrationPairChecker
from telegram_bot import TelegramBot

class CointegrationPairsFinder:

    NUM_STD_TO_ENTER = 1.5
    NUM_STD_TO_EXIT = 3

    def __init__(self,
                 tf: TimeFrame,
                 start: datetime,
                 end: datetime,
                 u: Universe,
                 dr: DataReader,
                 params: dict):
        self.tf = tf
        self.start = start
        self.end = end
        self.u = u
        self.dr = dr
        self.params = params

    def _read_dfs(self):
        tickers = self.u.get_tickers()

        self.dfs = {}
        for i, ticker in enumerate(tickers):
            df = self.dr.get_bars_df(ticker, self.tf, self.start, self.end)
            if df.shape[0] == 0:
                continue
            self.dfs[ticker] = df
            print(i, len(tickers), ticker, df.shape[0])

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
                df_is = df[:int(df.shape[0] * 0.75)]
                df_os = df[int(df.shape[0]  * 0.75):]

                cpc = CointegrationPairChecker(df_is, self.params)
                res, hedge_ratio, intercept, resid, info = cpc.cointegrate()
                if not res:
                    continue

                df_os.loc[:, "resid"] = df_os["close2"] - hedge_ratio * df_os["close1"] - intercept
                resid_std = np.std(resid)

                trade_res, ret, duration = self.get_trade_result(df_is, df_os, resid)
                info["trade_res"] = trade_res
                info["return"] = ret
                info["duration"] = duration
                if trade_res == "no_trade":
                    continue

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
                    marker=dict(color="purple"),
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
                fname = ROOT_FOLDER + "/output/cointegration_pairs_finder/{}_{}.html".format(t1, t2)
                fig = make_subplots(rows=4,
                                    cols=2,
                                    shared_yaxes=True,
                                    horizontal_spacing=0.0025,
                                    column_widths=[750, 250],
                                    specs=[
                                        [{}, {}],
                                        [{}, {}],
                                        [{"colspan": 2}, None],
                                        [{}, {}]
                                    ])
                fig.update_yaxes(range=[min(-self.NUM_STD_TO_EXIT * resid_std, resid_min), max(self.NUM_STD_TO_EXIT * resid_std, resid_max)], row=4, col=1)
                fig.update_yaxes(range=[min(-self.NUM_STD_TO_EXIT * resid_std, resid_min), max(self.NUM_STD_TO_EXIT * resid_std, resid_max)], row=4, col=2)
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
                fig.add_trace(regres_os, 3, 1)
                fig.add_trace(resid_is, 4, 1)
                fig.add_hline(y=self.NUM_STD_TO_EXIT * resid_std, row=4, col=1, line_dash="dash", line_color="red", line_width=1)
                fig.add_hline(y=self.NUM_STD_TO_ENTER * resid_std, row=4, col=1, line_dash="dash", line_color="green", line_width=1)
                fig.add_hline(y=-self.NUM_STD_TO_ENTER * resid_std, row=4, col=1, line_dash="dash", line_color="green", line_width=1)
                fig.add_hline(y=-self.NUM_STD_TO_EXIT * resid_std, row=4, col=1, line_dash="dash", line_color="red", line_width=1)
                fig.add_trace(resid_os, 4, 2)
                fig.add_hline(y=self.NUM_STD_TO_EXIT * resid_std, row=4, col=2, line_dash="dash", line_color="red", line_width=1)
                fig.add_hline(y=self.NUM_STD_TO_ENTER * resid_std, row=4, col=2, line_dash="dash", line_color="green", line_width=1)
                fig.add_hline(y=-self.NUM_STD_TO_ENTER * resid_std, row=4, col=2, line_dash="dash", line_color="green", line_width=1)
                fig.add_hline(y=-self.NUM_STD_TO_EXIT * resid_std, row=4, col=2, line_dash="dash", line_color="red", line_width=1)

                # Write html file.
                output_html_path = fname
                input_template_path = ROOT_FOLDER + "/templates/cointegration_pair.html"
                plotly_jinja_data = {
                    "fig": fig.to_html(full_html=False, default_height=1000),
                    "info": info
                }
                with open(output_html_path, "w", encoding="utf-8") as output_file:
                    with open(input_template_path) as template_file:
                        j2_template = Template(template_file.read())
                        output_file.write(j2_template.render(plotly_jinja_data))

                self.pairs.append([t1, t2])
                self.pairs_info.append({
                    "std": resid_std,
                    "trade_res": trade_res,
                    "duration": duration
                })
                print("\t", t1, t2, trade_res)

    def get_trade_result(self, df_is, df_os, resid):
        resid = resid.to_list()
        resid_std = np.std(resid)
        last_val = resid[-1]
        if -self.NUM_STD_TO_ENTER * resid_std <= last_val and last_val <= self.NUM_STD_TO_ENTER * resid_std:
            return "no_trade", 0, 0

        resid_out = df_os["resid"].to_list()
        if last_val < -resid_std:
            for i, e in enumerate(resid_out):
                if e < -self.NUM_STD_TO_EXIT * resid_std:
                    return "loss", -abs((e - last_val) / last_val), i
                if e > 0.0:
                    return "win", abs((e - last_val) / last_val), i

        if last_val > resid_std:
            for i, e in enumerate(resid_out):
                if e > self.NUM_STD_TO_EXIT * resid_std:
                    return "loss", -abs((last_val - e) / last_val), i
                if e < 0.0:
                    return "win", abs((last_val - e) / last_val), i

        return "unkown", 0, 0

    def get_num_tickers(self):
        return len(self.dfs.keys())

    def get_num_pairs(self):
        return len(self.pairs)

    def get_num_wins(self):
        res = 0
        for e in self.pairs_info:
            if e["trade_res"] == "win":
                res += 1
        return res

    def get_num_losses(self):
        res = 0
        for e in self.pairs_info:
            if e["trade_res"] == "loss":
                res += 1
        return res


    def send_found_pairs(self, source):
        tb = TelegramBot()
        tb.send_message("{}: {} pairs found. {} wins, {} losses.".format(source,
                                                                         self.get_num_pairs(),
                                                                         self.get_num_wins(),
                                                                         self.get_num_losses()))

        for pair in self.pairs:
            t1, t2 = pair
            title = "{}: {}_{} pair".format(source, t1, t2)
            fname = ROOT_FOLDER + "/output/cointegration_pairs_finder/{}_{}.html".format(t1, t2)
            tb.send_document(title, fname)
