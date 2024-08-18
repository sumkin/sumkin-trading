import talib
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from backtesting import Backtest, Strategy
from backtesting._util import _Array
from datetime import datetime, timedelta

from data_reader import DataReader
from universe import Universe

class MACrossoverStrategy(Strategy):
    FAST_PERIOD = 9
    SLOW_PERIOD = 21
    SL = 0.03
    TP = 0.09

    def init(self):
        self.data.df["fast_ma"] = self.data.df["Close"].ewm(span=MACrossoverStrategy.FAST_PERIOD).mean().values
        self.data.df["slow_ma"] = self.data.df["Close"].ewm(span=MACrossoverStrategy.SLOW_PERIOD).mean().values
        self.fast_ma = self.I(lambda x: x, _Array(self.data.df["fast_ma"].values, name="fast_ma"))
        self.slow_ma = self.I(lambda x: x, _Array(self.data.df["slow_ma"].values, name="slow_ma"))
        self.atr = self.I(lambda x: x,
                          _Array(talib.ATR(self.data.df["High"].values,
                                           self.data.df["Low"].values,
                                           self.data.df["Close"].values,
                                           timeperiod=20), name="atr"))

    def next(self):
        if self.fast_ma[-2] < self.slow_ma[-2] and self.fast_ma[-1] > self.slow_ma[-1]:
            # Long signal. Fast MA crosses slow MA from below to above.

            # Closing price should be above fast MA.
            if self.data.Close[-1] > self.fast_ma[-1]:
                # Difference between fast MA and the slow MA is less than the ATR.
                if self.fast_ma[-1] - self.slow_ma[-1] < self.atr[-1]:
                    self.buy(sl=(1.0 - MACrossoverStrategy.SL) * self.data.Close[-1],
                             tp=(1.0 + MACrossoverStrategy.TP)* self.data.Close[-1])
        elif self.fast_ma[-2] > self.slow_ma[-2] and self.fast_ma[-1] < self.slow_ma[-1]:
            # Short signal. Fast MA crosses slow MA from above to slow.

            # Closing prices should be below fast MA.
            if self.data.Close[-1] < self.fast_ma[-1]:
                # Difference between fast MA and the slow MA is less than the ATR.
                if self.fast_ma[-1] - self.slow_ma[-1] < self.atr[-1]:
                    self.sell(sl=(1.0 + MACrossoverStrategy.TP) * self.data.Close[-1],
                              tp=(1.0 - MACrossoverStrategy.SL) * self.data.Close[-1])

if __name__ == "__main__":
    end = datetime.now() - timedelta(days=1)
    start = (end - timedelta(days=365)).strftime("%Y-%m-%d %H:%M:%S")
    end = end.strftime("%Y-%m-%d %H:%M:%S")

    tickers = Universe().get_tickers()
    #tickers = ["ABBV"]
    cum_win_rate = 0.0
    for i, ticker in enumerate(tickers):
        dr = DataReader()
        df = dr.read_bars(ticker,
                          TimeFrame(1, TimeFrameUnit.Day),
                          start,
                          end)
        bt = Backtest(df, MACrossoverStrategy)
        stats = bt.run()
        win_rate = stats["Win Rate [%]"]
        cum_win_rate += win_rate
        print(i, ticker, win_rate, cum_win_rate / (i + 1))