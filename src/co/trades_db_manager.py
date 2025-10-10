import sqlite3
from datetime import datetime

class TradesDbManager:

    def __init__(self):
        self.conn = sqlite3.connect("/home/sumkin/sumkin-trading/data/trades.db")
        self.cursor = self.conn.cursor()

    def is_pair_active(self, symb1, symb2):
        q = '''
        SELECT active FROM cointegration_pair_trades 
        WHERE symb1 = "{}" AND symb2 = "{}"
        '''.format(symb1, symb2)
        self.cursor.execute(q)
        self.conn.commit()
        res = self.cursor.fetchall()
        if len(res) == 0:
            # No pairs traded.
            return False
        elif len(res) == 1:
            if res[0] == 0:
                return False
            else:
                return True
        else:
            assert False

    def get_active_pairs(self):
        q = '''
        SELECT id, symb1, symb2, hedge, coeff, sigma, side, amnt FROM cointegration_pair_trades WHERE active = 1 
        '''
        self.cursor.execute(q)
        self.conn.commit()
        pairs = self.cursor.fetchall()

        res = []
        for pair in pairs:
            res.append({
                "id": pair[0],
                "symb1": pair[1],
                "symb2": pair[2],
                "hedge": pair[3],
                "coeff": pair[4],
                "sigma": pair[5],
                "side": pair[6],
                "amnt": pair[7]
            })

        return res

    def add_trade(self, symb1, symb2, hedge, coeff, sigma, side, amnt, p1_enter, p2_enter):
        enter_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q = '''
        INSERT INTO cointegration_pair_trades 
        (symb1, symb2, hedge, coeff, sigma, enter_dt, side, amnt, active, p1_enter, p2_enter)
        VALUES 
        ("{}", "{}", {}, {}, {}, "{}", "{}", {}, {}, {}, {})
        '''.format(symb1, symb2, hedge, coeff, sigma, enter_dt, side, amnt, 1, p1_enter, p2_enter)
        self.cursor.execute(q)
        self.conn.commit()

    def close_trade(self, id, price1, price2):
        q = '''
        SELECT p1_enter, p2_enter, hedge, coeff, side, amnt 
        FROM cointegration_pair_trades 
        WHERE id = {}
        '''.format(id)
        self.cursor.execute(q)
        self.conn.commit()
        res = self.cursor.fetchall()
        assert len(res) == 1

        p1_enter, p2_enter, hedge, coeff, side, _ = res[0]
        assert side == "BUY" or side == "SELL"
        if side == "BUY":
            v_enter = p2_enter - hedge * p1_enter - coeff
            v_exit = price2 - hedge * price1 - coeff
        else:
            v_enter = -p2_enter + hedge * p1_enter + coeff
            v_exit = -price2 + hedge * price1 + coeff
        rtrn = v_exit - v_enter
        rtrn_pct = rtrn / v_enter

        exit_dt = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        q = '''
        UPDATE cointegration_pair_trades 
        SET exit_dt = "{}",
            return_pct = {},
            return = {},
            active = 0, 
            p1_exit = {},
            p2_exit = {}
        WHERE id = {}
        '''.format(exit_dt, rtrn_pct, rtrn, price1, price2, id)
        self.cursor.execute(q)
        self.conn.commit()

if __name__ == "__main__":
    tdm = TradesDbManager()
    #tdm.add_trade("XXX", "YYY", 1.5, 2.5, 10, "BUY", 10, 11.1, 22.2)
    #tdm.add_trade("XX1", "YY1", 1.5, 2.5, 10, "BUY", 10, 11.1, 22.2)
    #tdm.is_pair_active("USDGUSD", "USDQUSD")
    pairs = tdm.get_active_pairs()
    for pair in pairs:
        print(pair)



