import sys
import pytz
import pandas as pd
from datetime import datetime, timedelta
from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
from tinkoff.invest.schemas import CandleSource
sys.path.append("..")

from data_reader import DataReader
from tinkoff_universe import *
from time_frame import *

class TinkoffDataReader(DataReader):
    """
    Reads the data.
    """

    def __init__(self):
        pass

    def get_bars_df(self, ticker: str, tf: TimeFrame, start: datetime, end: datetime = None):
        instrument_id = TinkoffUniverse.get_figi_by_ticker(ticker)

        res_data = {
            "datetime": [],
            "open": [],
            "high": [],
            "low": [],
            "close": [],
            "volume": []
        }
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            NUM_ATTEMPTS = 5
            for _ in range(NUM_ATTEMPTS):
                try:
                    candles = client.get_all_candles(instrument_id=instrument_id,
                                                     from_=start,
                                                     to=end,
                                                     interval=TimeFrame.get_tinkoff_interval(tf),
                                                     candle_source_type=CandleSource.CANDLE_SOURCE_UNSPECIFIED)
                    break
                except:
                    time.sleep(5)

            for candle in candles:
                dt = candle.time.strftime("%Y-%m-%d %H:%M:%S")
                open = float(str(candle.open.units) + "." + str(candle.open.nano))
                high = float(str(candle.high.units) + "." + str(candle.high.nano))
                low = float(str(candle.low.units) + "." + str(candle.low.nano))
                close = float(str(candle.close.units) + "." + str(candle.close.nano))
                volume = float(str(candle.volume))
                res_data["datetime"].append(dt)
                res_data["open"].append(open)
                res_data["high"].append(high)
                res_data["low"].append(low)
                res_data["close"].append(close)
                res_data["volume"].append(volume)
        res_df = pd.DataFrame(data=res_data)
        return res_df

    def get_price(self, ticker: str, tf: TimeFrame, dt: datetime):
        instrument_id = TinkoffUniverse.get_figi_by_ticker(ticker)
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            candles = client.get_all_candles(instrument_id=instrument_id,
                                             from_=dt,
                                             to=dt,
                                             interval=TimeFrame.get_tinkoff_interval(tf),
                                             candle_source_type=CandleSource.CANDLE_SOURCE_UNSPECIFIED)
            assert len(list(candles)) == 0
            candle = list(candles)[0]
            return float(str(candle.close.units) + "." + str(candle.close.nano))

if __name__ == "__main__":
    tz = pytz.timezone("UTC")
    start = datetime.now() - timedelta(days=365)
    start = tz.localize(start)

    tu = TinkoffUniverse()
    tickers = tu.get_tickers()
    for ticker in tickers:
        print(ticker)
        tdr = TinkoffDataReader()
        df = tdr.get_bars_df(ticker, TimeFrame.INTERVAL_HOUR, start)
        print(df.head(5))
        print(df.tail(5))


