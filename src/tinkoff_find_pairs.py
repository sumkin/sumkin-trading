import pytz
from datetime import datetime, timedelta
from time_frame import TimeFrame

from tinkoff_universe import TinkoffUniverse
from tinkoff_data_reader import TinkoffDataReader
from cointegration_pairs_finder import CointegrationPairsFinder

if __name__ == "__main__":
    tz = pytz.timezone("UTC")
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=150)
    start = tz.localize(start)
    end = tz.localize(end)

    params = {
        "MIN_MONEY_VOLUME": 10e6,
        "MIN_MEAN_PRICE_RATIO": 0.5,
        "MAX_MEAN_PRICE_RATIO": 2,
        "MIN_HEDGE_RATIO": 0.5,
        "MAX_HEDGE_RATIO": 2,
        "MAX_ADFULLER": 0.005,
        "MIN_HOMOSCED_P_VAL": 0.05,
        "MIN_ZERO_MEAN_P_VAL": 0.01,
        "MIN_STD_RESID": 10
    }

    tu = TinkoffUniverse()
    tdr = TinkoffDataReader()
    cpf = CointegrationPairsFinder(TimeFrame.INTERVAL_DAY, start, end, tu, tdr, params)
    print("Reading dataframes...")
    cpf._read_dfs()
    print("Filtering by number of candles...")
    cpf._filter_by_num_candles()
    print("Filtering by volume...")
    cpf._filter_by_volume()
    print("Finding pairs...")
    cpf._find_pairs_split()
    print("Sending to Telegram...")
    cpf.send_found_pairs("Tinkoff")

    num_tickers = cpf.get_num_tickers()
    num_pairs = cpf.get_num_pairs()
    print("Number of tickers = {}".format(num_tickers))
    print("Number of pairs = {}".format(num_pairs))


