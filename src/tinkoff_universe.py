import re
import sys
from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX

from defs import ROOT_FOLDER
sys.path.append(ROOT_FOLDER)
from tinkoff_tokens import *

class TinkoffUniverse:

    def __init__(self):
        pass

    def get_shares(self):
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            instruments = client.instruments
            res = []
            for item in instruments.shares().instruments:
                if item.currency != "rub":
                    continue
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

    def get_figi_by_ticker(self, ticker):
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            instruments = client.instruments
            for item in instruments.shares().instruments:
                if item.ticker == ticker:
                    return item.figi
        return None

if __name__ == "__main__":
    tu = TinkoffUniverse()
    figi = tu.get_figi_by_ticker("BANEP")
    print("figi = {}".format(figi))