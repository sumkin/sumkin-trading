import sys
from time import sleep
from loguru import logger
from kraken.spot import Trade as SpotTrade
from kraken.spot import User as SpotUser
from kraken.futures import Trade as FuturesTrade
sys.path.append("/home/sumkin/sumkin-trading")
sys.path.append("/home/sumkin/sumkin-trading/src")

from kraken_api_keys import KRAKEN_SPOT_PRIVATE_KEY, KRAKEN_SPOT_PUBLIC_KEY
from kraken_api_keys import KRAKEN_FUTURES_PRIVATE_KEY, KRAKEN_FUTURES_PUBLIC_KEY
from kraken_universe import KrakenUniverse
from kraken_data_reader import KrakenDataReader
from krkn_order_handler import KrknOrderHandler
from kraken_cca_finder import KrakenCCAFinder
from cca_trades_db_manager import CCATradesDbManager

def find_cca_to_enter():
    kccaf = KrakenCCAFinder()
    res = []
    for spot_ticker, futures_ticker, spot_price, futures_price, vol in kccaf.find():
        # Overwrite volume. The deal should be around 10 USD (Should be removed later).
        new_vol = 10 / spot_price
        if new_vol < vol:
            vol = new_vol
        vol = round(vol, 1)
        e = {
            "spot_ticker": spot_ticker,
            "futures_ticker": futures_ticker,
            "spot_price": spot_price,
            "futures_price": futures_price,
            "vol": vol
        }
        res.append(e)
        if len(res) == 1:
            break
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
                yield id, spot_ticker, futures_ticker, spot_price_exit, futures_price_exit
    return

def enter_position_paper(spot_ticker, futures_ticker, spot_price, futures_price, vol):
    ccatdm = CCATradesDbManager(type="paper")
    if not ccatdm.is_pair_active(spot_ticker, futures_ticker):
        ccatdm.add_trade(spot_ticker, futures_ticker, spot_price, futures_price, vol)

def enter_position_real(spot_ticker, futures_ticker, spot_price, futures_price, vol):
    logger.info("enter_position_real() is called.")
    ccatdm = CCATradesDbManager(type="real")
    if not ccatdm.is_pair_active(spot_ticker, futures_ticker):
        logger.info("Pair is not active.")

        # Buy vol amount of spot_ticker with spot_price.
        logger.info("Buying spot {}".format(spot_ticker))
        spot_user = SpotUser(key=KRAKEN_SPOT_PUBLIC_KEY, secret=KRAKEN_SPOT_PRIVATE_KEY)
        spot_trade = SpotTrade(key=KRAKEN_SPOT_PUBLIC_KEY, secret=KRAKEN_SPOT_PRIVATE_KEY)
        spot_res = spot_trade.create_order(
            ordertype="market",
            volume=vol,
            side="buy",
            pair=spot_ticker
        )
        sleep(1)
        logger.info("spot_res = {}".format(spot_res))
        # FIXME: check that order is executed.
        spot_txid = spot_res["txid"][0]
        spot_order_status = spot_user.get_orders_info(txid=spot_txid)[spot_txid]["status"]

        # Sell vol amount of futures_ticker with futures_price.
        logger.info("Buying futures {}".format(futures_ticker))
        futures_trade = FuturesTrade(key=KRAKEN_FUTURES_PUBLIC_KEY, secret=KRAKEN_FUTURES_PRIVATE_KEY)
        futures_res = futures_trade.create_order(
            orderType="mkt",
            size=vol,
            side="sell",
            symbol=futures_ticker
        )
        sleep(1)
        # FIXME: check that order is executed.
        futures_order_id = futures_res["sendStatus"]["order_id"]
        futures_order_status = futures_trade.get_orders_status(orderIds=[futures_order_id])["orders"][0]["status"]

        if spot_order_status == "closed" and futures_order_status == "FULLY_EXECUTED":
            logger.info("Both orders executed.")
            ccatdm.add_trade(spot_ticker, futures_ticker, spot_price, futures_price, vol)
        else:
            assert False
    else:
        logger.info("Pair is active.")

def exit_position_paper(id, spot_ticker, futures_ticker, spot_price_exit, futures_price_exit):
    ccatdm = CCATradesDbManager(type="paper")
    ccatdm.close_trade(id, spot_price_exit, futures_price_exit)

def exit_position_real(id, spot_ticker, futures_ticker, spot_price_exit, futures_price_exit):
    ccatdm = CCATradesDbManager(type="real")
    info = ccatdm.get_trade_info(id)
    vol = info["vol"]

    # Sell vol amount of spot_ticker with spot_price_exit.
    spot_trade = SpotTrade(key=KRAKEN_SPOT_PUBLIC_KEY, secret=KRAKEN_SPOT_PRIVATE_KEY)
    spot_res = spot_trade.create_order(
        ordertype="market",
        volume=vol,
        side="sell",
        pair=spot_ticker
    )

    # Buy vol ammount of futures_ticker with futures_price.
    futures_trade = FuturesTrade(key=KRAKEN_FUTURES_PUBLIC_KEY, secret=KRAKEN_FUTURES_PRIVATE_KEY)
    futures_res = futures_trade.create_order(
        orderType="mkt",
        size=vol,
        side="buy",
        symbol=futures_ticker
    )

    ccatdm.close_trade(id, spot_price_exit, futures_price_exit)

if __name__ == "__main__":
    res = find_cca_to_exit()
    print(res)




