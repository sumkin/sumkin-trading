import pytz
from datetime import datetime, timedelta
from time_frame import TimeFrame

from kraken_universe import KrakenUniverse
from kraken_data_reader import KrakenDataReader
from cointegration_pairs_finder import CointegrationPairsFinder

if __name__ == "__main__":
    tz = pytz.timezone("UTC")
    end = datetime.now() - timedelta(days=1)
    start = end - timedelta(days=150)
    start = tz.localize(start)
    end = tz.localize(end)

    ku = KrakenUniverse()
    kdr = KrakenDataReader()
    cpf = CointegrationPairsFinder(TimeFrame.INTERVAL_DAY, start, end, ku, kdr)
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
