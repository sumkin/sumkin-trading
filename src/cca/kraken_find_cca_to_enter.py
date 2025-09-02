from loguru import logger

from kraken_cca_funcs import find_cca_to_enter, enter_position_real, enter_position_paper

if __name__ == "__main__":
    logger.add("/home/sumkin/sumkin-trading/output/cca_trades.log")
    logger.info("Finding cca trades to enter...")
    ccas = find_cca_to_enter()
    n = len(ccas)
    logger.info("Number of ccas = {}".format(n))
    for i in range(n):
        cca = ccas[i]
        spot_ticker = cca["spot_ticker"]
        futures_ticker = cca["futures_ticker"]
        spot_price = cca["spot_price"]
        futures_price = cca["futures_price"]
        vol = cca["vol"]
        logger.info("{}-{} {}-{}, {}".format(spot_ticker, futures_ticker, spot_price, futures_price, vol))
        logger.info("Entering real position...")
        enter_position_real(spot_ticker, futures_ticker, spot_price, futures_price, vol)
        logger.info("Entering paper position...")
        enter_position_paper(spot_ticker, futures_ticker, spot_price, futures_price, vol)

