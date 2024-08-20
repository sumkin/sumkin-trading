import re
import sys
import pandas as pd
from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
sys.path.append("..")

from tinkoff_tokens import *

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
                    res.append([item.ticker, item.class_code, item.name])
            return res

if __name__ == "__main__":
    tu = TinkoffUniverse()
    tickers = tu.get_brent_futures()
    for ticker, class_code, name in tickers:
        print(ticker, class_code, name)
