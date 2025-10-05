import sys
import json
from kraken.spot import Trade as SpotTrade
from kraken.spot import User as SpotUser
from kraken.futures import Trade as FuturesTrade
from kraken.futures import user as FuturesUser
sys.path.append("/home/sumkin/sumkin-trading")
sys.path.append("/home/sumkin/sumkin-trading/src")

from kraken_api_keys import KRAKEN_SPOT_PRIVATE_KEY, KRAKEN_SPOT_PUBLIC_KEY
from kraken_api_keys import KRAKEN_FUTURES_PRIVATE_KEY, KRAKEN_FUTURES_PUBLIC_KEY

if __name__ == "__main__":
    spot_user = SpotUser(key=KRAKEN_SPOT_PUBLIC_KEY, secret=KRAKEN_SPOT_PRIVATE_KEY)
    futures_trade = FuturesTrade(key=KRAKEN_FUTURES_PUBLIC_KEY, secret=KRAKEN_FUTURES_PRIVATE_KEY)
    spot_res = spot_user.get_trades_history()
    futures_res = futures_trade.get_fills()
    print(json.dumps(spot_res, indent=4))
