from kraken_cointegration_pairs_robot import find_pairs_to_enter, enter_position

if __name__ == "__main__":
    pairs, pairs_info = find_pairs_to_enter()
    assert len(pairs) == len(pairs_info)
    n = len(pairs)
    for i in range(n):
        pair = pairs[i]
        pair_info = pairs_info[i]
        enter_position(pair, pair_info)


