import sys
from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX

from defs import ROOT_FOLDER
sys.path.append(ROOT_FOLDER)
from tinkoff_tokens import *
from universe import Universe

ticker2figi_cache = {}

class TinkoffUniverse(Universe):

    def __init__(self):
        pass

    def get_tickers(self):
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            instruments = client.instruments
            res = []
            for item in instruments.shares().instruments:
                if item.currency != "rub":
                    continue
                res.append(item.ticker)
            return res

    @staticmethod
    def get_figi_by_ticker(ticker):
        # Check cache.
        if ticker in ticker2figi_cache:
            return ticker2figi_cache[ticker]

        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            instruments = client.instruments
            for item in instruments.shares().instruments:
                if item.ticker == ticker:
                    ticker2figi_cache[ticker] = item.figi
                    return item.figi
        return None

if __name__ == "__main__":
    figi = TinkoffUniverse.get_figi_by_ticker("BANEP")
    print("figi = {}".format(figi))