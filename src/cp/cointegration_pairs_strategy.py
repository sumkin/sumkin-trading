import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import statsmodels.formula.api as smf

from position import Position
from time_frame import TimeFrame
from tinkoff_universe import TinkoffUniverse
from tinkoff_data_reader import TinkoffDataReader

class CointegrationPairsStrategy:
    BUY = 1
    SELL = 2
    CLOSE = 3
    HOLD = 4

    def __init__(self, ticker1, ticker2):
        self.ticker1 = ticker1
        self.ticker2 = ticker2
        self.look_back = timedelta(days=365)
        self.position = Position()

    def signal(self, dt: datetime):
        start = dt - timedelta(days=1) - self.look_back
        end = dt - timedelta(days=1)

        tdr = TinkoffDataReader()
        df1 = tdr.get_bars_df(self.ticker1, TimeFrame.INTERVAL_DAY, start, end)
        df2 = tdr.get_bars_df(self.ticker2, TimeFrame.INTERVAL_DAY, start, end)
        df = pd.merge(df1, df2, on="datetime", suffixes=("1", "2")).dropna()
        fit = smf.ols("close2 ~ close1", data=df).fit()

        mean = np.std(fit.resid)
        std = np.std(fit.resid)
        hedge_ratio = fit.params["close1"]

        p1 = tdr.get_price(self.ticker1, TimeFrame.INTERVAL_DAY, dt)
        p2 = tdr.get_price(self.ticker2, TimeFrame.INTERVAL_DAY, dt)
        resid = p2 - hedge_ratio * p1

        if self.position.is_empty():
            if resid + mean < -std:
                # Buy hedge.
                self.position.buy(self.ticker1, p1, 1)

                return CointegrationPairsStrategy.BUY
            elif resid + mean > std:
                # Sell hedge.
                return CointegrationPairsStrategy.SELL
        else:
            # Position is not empty.
            if abs(resid - mean) / std < 0.1:
                # Close the position.
                return CointegrationPairsStrategy.CLOSE

        return CointegrationPairsStrategy.HOLD


if __name__ == "__main__":
    dt = datetime(2024, 8, 29)
    cps = CointegrationPairsStrategy("BANEP", "NVTK")
    cps.signal(dt)




