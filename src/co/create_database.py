import sqlite3 

def create_cointegration_pair_trades_table():
    conn = sqlite3.connect("/home/sumkin/sumkin-trading/data/trades.db")
    cursor = conn.cursor()
    q = '''CREATE TABLE IF NOT EXISTS cointegration_pair_trades ( 
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                symb1 TEXT NOT NULL,
                symb2 TEXT NOT NULL,
                hedge REAL,
                coeff REAL,
                sigma REAL,
                enter_dt TEXT,
                exit_dt TEXT,
                side TEXT,
                amnt REAL,
                return_pct REAL,
                return REAL,
                active INTEGER,
                p1_enter REAL,
                p2_enter REAL,
                p1_exit REAL,
                p2_exit REAL)
        '''
    cursor.execute(q)
    conn.commit()

if __name__ == "__main__":
    create_cointegration_pair_trades_table()
