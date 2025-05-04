import numpy as np
from datetime import datetime, timedelta 
from time_frame import TimeFrame

from kraken_universe import KrakenUniverse 
from kraken_data_reader import KrakenDataReader 
from cointegration_pairs_finder import CointegrationPairsFinder
from trades_db_manager import TradesDbManager

def find_pairs_to_enter():
    interval = TimeFrame.INTERVAL_HOUR 
    mins = TimeFrame.get_num_minutes(interval)
    end = datetime.now()
    start = end - timedelta(minutes=mins*112)  # 150 * 0.75

    params = {
        "MIN_MONEY_VOLUME": 3000,
        "MIN_MEAN_PRICE_RATIO": 0.1,
        "MAX_MEAN_PRICE_RATIO": 1.0,
        "MIN_HEDGE_RATIO": 0.2,
        "MAX_HEDGE_RATIO": 5.0,
        "MAX_ADFULLER": 0.05,
        "MIN_HOMOSCED_P_VAL": 0.05,
        "MIN_ZERO_MEAN_P_VAL": 0.05,
        "MIN_STD_RESID": 0.0
    }

    ku = KrakenUniverse()
    kdr = KrakenDataReader()
    cpf = CointegrationPairsFinder(interval, start, end, ku, kdr, params, create_html=False)
    print("Reading dataframes...")
    cpf._read_dfs()
    print("Filtering by number of candles...")
    cpf._filter_by_num_candles()
    print("Filtering by volume...")
    cpf._filter_by_volume()
    print("Finding pairs...")
    cpf._find_pairs()

    return cpf.pairs, cpf.pairs_info

def find_pairs_to_exit():
    interval = TimeFrame.INTERVAL_5_MIN
    kdr = KrakenDataReader()

    tdm = TradesDbManager()
    pairs = tdm.get_active_pairs()
    for pair in pairs:
        symb1, symb2 = pair["symb1"], pair["symb2"]
        price1 = kdr.get_bars_df(symb1, interval, datetime.now() - timedelta(minutes=15))["close"].to_list()[-1]
        price2 = kdr.get_bars_df(symb2, interval, datetime.now() - timedelta(minutes=15))["close"].to_list()[-1]

        id = pair["id"]
        hedge = pair["hedge"]
        coeff = pair["coeff"]
        sigma = pair["sigma"]
        side = pair["side"]
        amnt = pair["amnt"]

        resid = price2 - hedge * price1 - coeff
        if side == "BUY":
            if resid < 0:
                exit_position(id, price1, price2)
            elif resid > CointegrationPairsFinder.NUM_STD_TO_EXIT * sigma:
                exit_position(id, price1, price2)
            else:
                pass
        elif side == "SELL":
            if resid > 0:
                exit_position(id, price1, price2)
            elif resid < -CointegrationPairsFinder.NUM_STD_TO_EXIT * sigma:
                exit_position(id, price1, price2)
            else:
                pass
        else:
            assert False

def enter_position(pair, pair_info):
    symb1, symb2 = pair
    sigma = pair_info["resid_std"]
    hedge = pair_info["hedge_ratio"]
    coeff = pair_info["intercept"]
    p1 = pair_info["p1"]
    p2 = pair_info["p2"]

    tdm = TradesDbManager()

    resid = p2 - hedge * p1 - coeff
    if abs(resid) > CointegrationPairsFinder.NUM_STD_TO_ENTER * sigma:
        if resid > 0:
            # Buy symb1 and sell symb2.
            if not tdm.is_pair_active(symb1, symb2):
                tdm.add_trade(symb1, symb2, hedge, coeff, sigma, "BUY", 1.0, p1, p2)
        else:
            # Sell symb1 and buy symb2.
            if not tdm.is_pair_active(symb1, symb2):
                tdm.add_trade(symb1, symb2, hedge, coeff, sigma, "SELL", 1.0, p1, p2)

def exit_position(id, price1, price2):
    tdm = TradesDbManager()
    tdm.close_trade(id, price1, price2)

if __name__ == "__main__":
    """
    pairs, pairs_info = find_pairs_to_enter()
    assert len(pairs) == len(pairs_info)
    n = len(pairs)
    for i in range(n):
        pair = pairs[i]
        pair_info = pairs_info[i]
        enter_position(pair, pair_info)
    """
    find_pairs_to_exit()