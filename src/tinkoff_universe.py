import re
import sys
import pytz
from datetime import datetime as dt, timedelta
from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
sys.path.append("..")

from time_frame import TimeFrame
from tinkoff_tokens import *
from tinkoff_data_reader import TinkoffDataReader

class TinkoffUniverse:

    def __init__(self):
        pass

    def get_shares(self):
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            instruments = client.instruments
            res = []
            for item in instruments.shares().instruments:
                res.append([item.ticker, item.figi])
            return res

    def get_brent_futures(self):
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            instruments = client.instruments
            res = []
            for item in instruments.futures().instruments:
                match = re.findall("BR-[0-9]{1,2}\.[0-9]{2}", item.name)
                if len(match) != 0:
                    assert len(match) == 1
                    month, year = match[0].split("-")[1].split(".")
                    month, year = int(month), int(year)
                    res.append([item.ticker, item.class_code, item.figi, item.name, month, year])
            return res

if __name__ == "__main__":
    tu = TinkoffUniverse()
    tickers = tu.get_brent_futures()
    for ticker, class_code, figi, name, month, year in tickers:
        print(ticker, class_code, figi, name, month, year)
