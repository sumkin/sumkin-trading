import sqlite3

def create_cca_database():
    conn = sqlite3.connect("../../data/cca_trades.db")
    cursor = conn.cursor()
    q = '''
    CREATE TABLE IF NOT EXISTS cca_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        spot_ticker TEXT NOT NULL,
        futures_ticker TEXT NOT NULL,
        bid REAL,
        ask REAL,
        vol REAL,
        enter_dt TEXT,
        exit_dt TEXT)
    '''
    cursor.execute(q)
    conn.commit()

if __name__ == "__main__":
    create_cca_database()

