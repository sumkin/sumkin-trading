from tinkoff_universe import TinkoffUniverse
from kraken_universe import KrakenUniverse

def test_tinkoff_universe():
    tu = TinkoffUniverse()
    tickers = tu.get_tickers()
    assert len(tickers) >= 5 # There should be some tickers.

def test_kraken_universe():
    ku = KrakenUniverse()
    tickers = ku.get_tickers()
    assert len(tickers) >= 5 # There should be some tickers.
