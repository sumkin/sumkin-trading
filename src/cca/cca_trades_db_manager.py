import sqlite3
from datetime import datetime

class CCATradesDbManager:

    def __init__(self):
        self.conn = sqlite3.connect("/home/sumkin/sumkin-trading/data/cca_trades.db")
        self.cursor = self.conn.cursor()

    def is_pair_active(self, spot_ticker, futures_ticker):
        q = '''
        SELECT COUNT(*) FROM cca_trades
        WHERE spot_ticker = "{}" AND futures_ticker = "{}" AND active = 1
        '''.format(spot_ticker, futures_ticker)
        self.cursor.execute(q)
        self.conn.commit()
        res = self.cursor.fetchall()
        if len(res) == 0:
            return False
        elif len(res) == 1:
            assert res[0][0] == 1
            return True
        else:
            assert False

    def get_active_trades(self):
        q = '''
        SELECT id, spot_ticker, futures_ticker FROM cca_trades WHERE active = 1
        '''
        self.cursor.execute(q)
        self.conn.commit()
        pairs = self.cursor.fetchall()

        res = []
        for pair in pairs:
            res.append({
                "id": pair[0],
                "spot_ticker": pair[1],
                "futures_ticker": pair[2]
            })

        return res

    def add_trade(self, spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol):
        enter_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q = '''
        INSERT INTO cca_trades
        (spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, active)
        VALUES 
        ("{}", "{}", {}, {}, {}, {})
        '''.format(spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, 1)
        self.cursor.execute(q)
        self.conn.commit()

    def close_trade(self, id, spot_price_exit, futures_price_exit):
        q = '''
        SELECT * FROMM cca_trades WHERE id = {}
        '''.format(id)
        self.cursor.execute(q)
        self.conn.commit()
        res = self.cursor.fetchall()
        assert len(res) == 1

        exit_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q = '''
        UPDATE cca_trades
        SET spot_price_exit = {},
            futures_price_exit = {},
            exit_dt = "{}",
            active = 0
        WHERE id = {}
        '''.format(spot_price_exit, futures_price_exit, exit_dt, id)
        self.cursor.execute(q)
        self.conn.commit()

if __name__ == "__main__":
    ccatdm = CCATradesDbManager()
    pairs = ccatdm.get_active_trades()
    for pair in pairs:
        print(pair)