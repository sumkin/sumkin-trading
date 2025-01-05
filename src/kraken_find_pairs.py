import pytz
import numpy as np
from datetime import datetime, timedelta
from time_frame import TimeFrame

from kraken_universe import KrakenUniverse
from kraken_data_reader import KrakenDataReader
from cointegration_pairs_finder import CointegrationPairsFinder

if __name__ == "__main__":
    interval = TimeFrame.INTERVAL_HOUR
    mins = TimeFrame.get_num_minutes(interval)
    end = datetime.now()
    start = end - timedelta(minutes=mins*150)

    params = {
        "MIN_MONEY_VOLUME": 3000,
        "MIN_MEAN_PRICE_RATIO": 0.1,
        "MAX_MEAN_PRICE_RATIO": 1.0,
        "MIN_HEDGE_RATIO": 0.2,
        "MAX_HEDGE_RATIO": 5.0,
        "MAX_ADFULLER": 0.05,
        "MIN_HOMOSCED_P_VAL": 0.05,
        "MIN_ZERO_MEAN_P_VAL": 0.05,
        "MIN_STD_RESID": 0.0
    }

    ku = KrakenUniverse()
    kdr = KrakenDataReader()
    cpf = CointegrationPairsFinder(interval, start, end, ku, kdr, params, create_html=False)
    print("Reading dataframes...")
    cpf._read_dfs()
    print("Filtering by number of candles...")
    cpf._filter_by_num_candles()
    print("Filtering by volume...")
    cpf._filter_by_volume()
    print("Finding pairs...")
    cpf._find_pairs()
    print("Sending to Telegram...")
    cpf.send_found_pairs("Kraken")

    num_tickers = cpf.get_num_tickers()
    num_pairs = cpf.get_num_pairs()
    print("Number of tickers = {}".format(num_tickers))
    print("Number of pairs = {}".format(num_pairs))
