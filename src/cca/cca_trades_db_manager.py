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
        assert len(res) == 1
        res = res[0]

        if res[0] == 0:
            return False
        elif res[0] == 1:
            return True
        else:
            print(res)
            assert False

    def get_active_trades(self):
        q = '''
        SELECT id, spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol FROM cca_trades WHERE active = 1
        '''
        self.cursor.execute(q)
        self.conn.commit()
        ccas = self.cursor.fetchall()

        res = []
        for cca in ccas:
            res.append({
                "id": cca[0],
                "spot_ticker": cca[1],
                "futures_ticker": cca[2],
                "spot_price_enter": cca[3],
                "futures_price_enter": cca[4],
                "vol": cca[5]
            })

        return res

    def add_trade(self, spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol):
        enter_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q = '''
        INSERT INTO cca_trades
        (spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, active)
        VALUES 
        ("{}", "{}", {}, {}, {}, "{}", {})
        '''.format(spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, 1)
        self.cursor.execute(q)
        self.conn.commit()

    def close_trade(self, id, spot_price_exit, futures_price_exit):
        q = '''
        SELECT * FROM cca_trades WHERE id = {}
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