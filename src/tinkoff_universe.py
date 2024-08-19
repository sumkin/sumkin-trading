import sys
import pandas as pd
from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
sys.path.append("..")

from tinkoff_tokens import *

class TinkoffUniverse:

    def __init__(self):
        pass

    def get_shares_figis(self):
        with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
            instruments = client.instruments
            res = []
            for item in instruments.shares().instruments:
                res.append(item.figi)
            return res

if __name__ == "__main__":
    tu = TinkoffUniverse()
    figis = tu.get_shares_figis()
    print(len(figis))
    print(figis)
