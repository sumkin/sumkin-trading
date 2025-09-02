import sqlite3
from datetime import datetime

class CCATradesDbManager:

    def __init__(self, type):
        assert type == "paper" or type == "real"
        self.type = type
        if self.type == "paper":
            self.conn = sqlite3.connect("/home/sumkin/sumkin-trading/data/cca_trades_paper.db")
            self.cursor = self.conn.cursor()
        elif self.type == "real":
            self.conn = sqlite3.connect("/home/sumkin/sumkin-trading/data/cca_trades_real.db")
            self.cursor = self.conn.cursor()
        else:
            assert False

    def is_pair_active(self, spot_ticker, futures_ticker):
        if self.type == "paper":
            q = '''
            SELECT COUNT(*) FROM cca_trades_paper
            WHERE spot_ticker = "{}" AND futures_ticker = "{}" AND active = 1
            '''.format(spot_ticker, futures_ticker)
        elif self.type == "real":
            q = '''
            SELECT COUNT(*) FROM cca_trades_real
            WHERE spot_ticker = "{}" AND futures_ticker = "{}" AND active = 1
            '''.format(spot_ticker, futures_ticker)
        else:
            assert False
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
        if self.type == "paper":
            q = '''
            SELECT id, spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol FROM cca_trades_paper WHERE active = 1
            '''
        elif self.type == "real":
            q = '''
            SELECT id, spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol FROM cca_trades_real WHERE active = 1
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
        if self.type == "paper":
            q = '''
            INSERT INTO cca_trades_paper
            (spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, active)
            VALUES 
            ("{}", "{}", {}, {}, {}, "{}", {})
            '''.format(spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, 1)
        elif self.type == "real":
            q = '''
            INSERT INTO cca_trades_real
            (spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, active)
            VALUES 
            ("{}", "{}", {}, {}, {}, "{}", {})
            '''.format(spot_ticker, futures_ticker, spot_price_enter, futures_price_enter, vol, enter_dt, 1)
        else:
            assert False
        self.cursor.execute(q)
        self.conn.commit()

    def close_trade(self, id, spot_price_exit, futures_price_exit):
        if self.type == "paper":
            q = '''
            SELECT * FROM cca_trades_paper WHERE id = {}
            '''.format(id)
        elif self.type == "real":
            q = '''
            SELECT * FROM cca_trades_real WHERE id = {}
            '''.format(id)
        else:
            assert False
        self.cursor.execute(q)
        self.conn.commit()
        res = self.cursor.fetchall()
        assert len(res) == 1, "q = {}".format(q)

        exit_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        if self.type == "paper":
            q = '''
            UPDATE cca_trades_paper
            SET spot_price_exit = {},
                futures_price_exit = {},
                exit_dt = "{}",
                active = 0
            WHERE id = {}
            '''.format(spot_price_exit, futures_price_exit, exit_dt, id)
        elif self.type == "real":
            q = '''
            UPDATE cca_trades_real
            SET spot_price_exit = {},
                futures_price_exit = {},
                exit_dt = "{}",
                active = 0
            WHERE id = {}
            '''.format(spot_price_exit, futures_price_exit, exit_dt, id)
        else:
            assert False
        self.cursor.execute(q)
        self.conn.commit()

    def get_trade_info(self, id):
        if self.type == "paper":
            q = "SELECT * FROM cca_trades_paper WHERE id = {}".format(id)
        elif self.type == "real":
            q = "SELECT vol FROM cca_trades_real WHERE id = {}".format(id)
        else:
            assert False
        self.cursor.execute(q)
        self.conn.commit()
        res = self.cursor.fetchall()
        assert len(res) == 1, "q = {}".format(q)

        info = {
            "vol": res[0][0]
        }
        return info

if __name__ == "__main__":
    ccatdm = CCATradesDbManager(type="paper")
    pairs = ccatdm.get_active_trades()
    for pair in pairs:
        print(pair)