import sys
from tinkoff.invest import Client
from tinkoff.invest.constants import INVEST_GRPC_API_SANDBOX
sys.path.append("..")

from tinkoff_tokens import *

with Client(tinkoff_sandbox_token, target=INVEST_GRPC_API_SANDBOX) as client:
    instruments = client.instruments
    tickers = []
    for method in ["shares", "bonds", "etfs", "currencies", "futures"]:
        for item in getattr(instruments, method)().instruments:
            print(item.ticker)
