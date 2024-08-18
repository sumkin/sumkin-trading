import numpy as np
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from backtesting import Backtest, Strategy
from datetime import datetime, timedelta
from mpire import WorkerPool
from multiprocessing import cpu_count

from data_reader import DataReader
from universe import Universe
from ma_crossover_strategy import MACrossoverStrategy

def optimize_sl_tp():
    end = datetime(2024,8,14)
    start = (end - timedelta(days=365)).strftime("%Y-%m-%d") + " 00:00:00"
    end = end.strftime("%Y-%m-%d") + " 23:59:59"

    tickers = Universe().get_tickers()

    max_sl = None
    max_tp = None
    max_win_rate = -np.inf
    for sl in np.arange(0.01, 0.1, 0.01):
        for tp in np.arange(0.01, 0.1, 0.01):
            def process(ticker):
                dr = DataReader()
                df = dr.read_bars(ticker,
                                  TimeFrame(1, TimeFrameUnit.Day),
                                  start,
                                  end)
                MACrossoverStrategy.SL = sl
                MACrossoverStrategy.TP = tp
                bt = Backtest(df, MACrossoverStrategy)
                stats = bt.run()
                win_rate = stats["Win Rate [%]"]
                return win_rate

            with WorkerPool(n_jobs=cpu_count() - 1) as pool:
                win_rates = pool.map(process, tickers)

            mean_win_rate = np.mean(win_rates)
            if mean_win_rate > max_win_rate:
                max_sl = sl
                max_tp = tp
                max_win_rate = mean_win_rate
            print("{}, {}: Mean, std win rate = {}, {}".format(sl, tp, np.mean(win_rates), np.std(win_rates)))
    print("max_sl = {}".format(max_sl))
    print("max_tp = {}".format(max_tp))

if __name__ == "__main__":
    optimize_sl_tp()