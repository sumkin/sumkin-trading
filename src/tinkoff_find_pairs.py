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

    tu = TinkoffUniverse()
    tdr = TinkoffDataReader()
    cpf = CointegrationPairsFinder(TimeFrame.INTERVAL_DAY, start, end, tu, tdr)
    print("Reading dataframes...")
    cpf._read_dfs()
    print("Filtering by number of candles...")
    cpf._filter_by_num_candles()
    print("Filtering by volume...")
    cpf._filter_by_volume()
    print("Finding pairs...")
    cpf._find_pairs()
    print("Sending to Telegram...")
    cpf.send_found_pairs("Tinkoff")

    num_tickers = cpf.get_num_tickers()
    num_pairs = cpf.get_num_pairs()
    print("Number of tickers = {}".format(num_tickers))
    print("Number of pairs = {}".format(num_pairs))


