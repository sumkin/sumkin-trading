from kraken_cca_funcs import find_cca_to_enter, enter_position

if __name__ == "__main__":
    ccas = find_cca_to_enter()
    n = len(ccas)
    for i in range(n):
        cca = ccas[i]
        spot_ticker = cca["spot_ticker"]
        futures_ticker = cca["futures_ticker"]
        spot_price = cca["spot_price"]
        futures_price = cca["futures_price"]
        vol = cca["vol"]
        enter_position(spot_ticker, futures_ticker, spot_price, futures_price, vol)

