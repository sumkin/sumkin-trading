import sqlite3

def create_cca_database():
    conn = sqlite3.connect("../../data/cca_trades.db")
    cursor = conn.cursor()
    q = '''
    CREATE TABLE IF NOT EXISTS cca_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        spot_ticker TEXT NOT NULL,
        futures_ticker TEXT NOT NULL,
        spot_price_enter REAL,
        futures_price_enter REAL,
        spot_price_exit REAL,
        futures_price_exit REAL,
        vol REAL,
        enter_dt TEXT,
        exit_dt TEXT,
        active INTEGER)
    '''
    cursor.execute(q)
    conn.commit()

if __name__ == "__main__":
    create_cca_database()

