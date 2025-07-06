from kraken_cca_funcs import find_cca_to_exit, exit_position_paper

if __name__ == "__main__":
    for id, spot_price_exit, futures_price_exit in find_cca_to_exit(type="paper"):
        exit_position_paper(id, spot_price_exit, futures_price_exit)

