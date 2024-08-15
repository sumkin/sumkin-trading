from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from backtesting import Backtest, Strategy
from backtesting._util import _Array
from datetime import datetime, timedelta

from data_reader import DataReader

class MACrossoverStrategy(Strategy):
    FAST_PERIOD = 20
    SLOW_PERIOD = 50

    def init(self):
        self.data.df["fast_ma"] = self.data.df["Close"].ewm(span=MACrossoverStrategy.FAST_PERIOD).mean().values
        self.data.df["slow_ma"] = self.data.df["Close"].ewm(span=MACrossoverStrategy.SLOW_PERIOD).mean().values
        self.fast_ma = self.I(lambda x: x, _Array(self.data.df["fast_ma"].values, name="fast_ma"))
        self.slow_ma = self.I(lambda x: x, _Array(self.data.df["slow_ma"].values, name="slow_ma"))

    def next(self):
        if self.fast_ma[-2] < self.slow_ma[-2] and self.fast_ma[-1] > self.slow_ma[-1]:
            self.buy()
        elif self.slow_ma[-2] > self.slow_ma[-2] and self.fast_ma[-1] < self.slow_ma[-1]:
            self.sell()

if __name__ == "__main__":
    end = datetime.now() - timedelta(days=1)
    start = (end - timedelta(days=14)).strftime("%Y-%m-%d %H:%M:%S")
    end = end.strftime("%Y-%m-%d %H:%M:%S")

    dr = DataReader()
    df = dr.read_bars("TSLA",
                      TimeFrame(5, TimeFrameUnit.Minute),
                      start,
                      end)
    bt = Backtest(df, MACrossoverStrategy)
    bt.run()
    bt.plot(filename="../output/ma_crossover.html")