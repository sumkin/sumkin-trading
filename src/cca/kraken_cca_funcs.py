import sys

sys.path.append("/home/sumkin/sumkin-trading/src")

from kraken_universe import KrakenUniverse
from kraken_data_reader import KrakenDataReader
from krkn_order_handler import KrknOrderHandler
from kraken_cca_finder import KrakenCCAFinder
from cca_trades_db_manager import CCATradesDbManager

def find_cca_to_enter():
    kccaf = KrakenCCAFinder()
    res = []
    for spot_ticker, futures_ticker, spot_price, futures_price, vol in kccaf.find():
        e = {
            "spot_ticker": spot_ticker,
            "futures_ticker": futures_ticker,
            "spot_price": spot_price,
            "futures_price": futures_price,
            "vol": vol
        }
        res.append(e)
    return res

def find_cca_to_exit(type):
    assert type == "paper" or type == "real"
    kdr = KrakenDataReader()
    ccatdm = CCATradesDbManager(type)
    ccas = ccatdm.get_active_trades()
    for cca in ccas:
        id = cca["id"]
        spot_ticker = cca["spot_ticker"]
        futures_ticker = cca["futures_ticker"]
        spot_price_enter = cca["spot_price_enter"]
        futures_price_enter = cca["futures_price_enter"]
        vol = cca["vol"]

        spot_price_exit, spot_vol = kdr.get_best_ask(spot_ticker, market="spot")
        futures_price_exit, futures_vol = kdr.get_best_bid(futures_ticker, market="futures")
        if min(spot_vol, futures_vol) >= vol:
            spot_profit = (spot_price_exit - spot_price_enter) * vol
            futures_profit = (futures_price_enter - futures_price_exit) * vol
            profit = spot_profit + futures_profit
            commission = (spot_price_enter + futures_price_enter + spot_price_exit + futures_price_exit) * vol * 0.003
            if profit > commission:
                yield id, spot_price_exit, futures_price_exit
    return

def enter_position_paper(spot_ticker, futures_ticker, spot_price, futures_price, vol):
    ccatdm = CCATradesDbManager(type="paper")
    if not ccatdm.is_pair_active(spot_ticker, futures_ticker):
        ccatdm.add_trade(spot_ticker, futures_ticker, spot_price, futures_price, vol)

def enter_position_real(spot_ticker, futures_ticker, spot_price, futures_price, vol):
    ccatdm = CCATradesDbManager(type="real")
    if not ccatdm.is_pair_active(spot_ticker, futures_ticker):
        # Buy vol amount of spot_ticker with spot_price.
        pass

def exit_position_paper(id, spot_price_exit, futures_price_exit):
    ccatdm = CCATradesDbManager(type="paper")
    ccatdm.close_trade(id, spot_price_exit, futures_price_exit)

def exit_position_real(id, spot_price_exit, futures_price_exit):
    pass

if __name__ == "__main__":
    res = find_cca_to_exit()
    print(res)




