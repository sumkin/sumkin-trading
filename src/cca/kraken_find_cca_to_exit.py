from loguru import logger

from kraken_cca_funcs import find_cca_to_exit, exit_position_paper, exit_position_real

if __name__ == "__main__":
    logger.info("Finding pairs to exit...")
    for id, spot_ticker, futures_ticker, spot_price_exit, futures_price_exit in find_cca_to_exit(type="real"):
        logger.info("Exiting {}-{}...".format(spot_ticker, futures_ticker))
        exit_position_real(id, spot_ticker, futures_ticker, spot_price_exit, futures_price_exit)
        exit_position_paper(id, spot_ticker, futures_ticker, spot_price_exit, futures_price_exit)

