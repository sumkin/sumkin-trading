import sqlite3

def create_cca_database():
    conn = sqlite3.connect("../data/cca_trades.db")
    cursor = conn.cursor()
    q = '''
    CREATE TABLE IF NOT EXISTS cca_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        
    )
    '''

