from alpaca.data.timeframe import TimeFrameUnit
from alpaca.data.timeframe import TimeFrame

from data_reader import DataReader

def test_data_reader_read():
    dr = DataReader()
    df = dr.read_bars("TSLA",
                      TimeFrame(1, TimeFrameUnit.Day),
                      "2023-01-01 00:00:00",
                      "2024-01-01 00:00:00")
    assert df.shape[0] > 0

