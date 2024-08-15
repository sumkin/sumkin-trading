from universe import Universe

def test_universe():
    universe = Universe()
    tickers = universe.get_tickers()
    assert len(tickers) >= 500

