import clickhouse_driver
from datetime import datetime
import pandas as pd

def setup_clickhouse():
    # Connection settings
    client = clickhouse_driver.Client(
        host='localhost',
        port=9000,
        user='default',
        password='',  # Add your password if needed
        database='default'
    )

    # Check connection
    try:
        client.execute("SELECT 1")
        print("Successfully connected to ClickHouse!")
    except Exception as e:
        print(f"Connection error: {e}")
        return

    # Create database if not exists
    client.execute("CREATE DATABASE IF NOT EXISTS order_book_predictor")
    client.execute("USE order_book_predictor")

    # Drop tables if they exist (for testing purposes)
    client.execute("DROP TABLE IF EXISTS order_book_data")

    # Create the order book data table with candles
    create_table_query = """
    CREATE TABLE IF NOT EXISTS order_book_data (
        timestamp DateTime64(6),
        instrument_id String,

        -- Order book data
        asks_prices Array(Float64),
        asks_quantities Array(Float64),
        bids_prices Array(Float64),
        bids_quantities Array(Float64),

        opens Array(Float64),
        highs Array(Float64),
        lows Array(Float64),
        closes Array(Float64),
        volumes Array(Float64)
    ) 
    ENGINE = MergeTree()
    ORDER BY (instrument_id, timestamp)
    """

    client.execute(create_table_query)
    print("Table created successfully!")

def insert_sample_data():
    # Create a client for the order_book_predictor database
    client = clickhouse_driver.Client(
        host='localhost',
        port=9000,
        user='default',
        password='',  # Add your password if needed
        database='order_book_predictor'
    )

    sample_data = {
        'timestamp': datetime.now(),
        'instrument_id': 'SBER',

        # Order book
        'asks_prices': [100.1, 100.2, 100.3, 100.4, 100.5],
        'asks_quantities': [10, 20, 30, 40, 50],
        'bids_prices': [99.9, 99.8, 99.7, 99.6, 99.5],
        'bids_quantities': [15, 25, 35, 45, 55],

        # Candles
        'opens': [99.8, 99.5, 99.0, 98.5, 98.0, 97.5, 97.0, 96.5, 96.0],
        'highs': [100.3, 100.0, 99.5, 99.0, 98.5, 98.0, 97.5, 97.0, 96.5],
        'lows': [99.3, 99.0, 98.5, 98.0, 97.5, 97.0, 96.5, 96.0, 95.5],
        'closes': [99.5, 99.2, 98.7, 98.2, 97.7, 97.2, 96.7, 96.2, 95.7],
        'volumes': [950, 900, 850, 800, 750, 700, 650, 600, 550]
    }

    client.execute(
        "INSERT INTO order_book_data VALUES",
        [sample_data]
    )
    print("Sample data inserted successfully!")

    # Query to check the data
    result = client.execute("SELECT * FROM order_book_data")
    df = pd.DataFrame(result, columns=[
        'timestamp', 'instrument_id',
        'asks_prices', 'asks_quantities', 'bids_prices', 'bids_quantities',
        'opens', 'highs', 'lows', 'closes', 'volumes'
    ])
    print("Verifying inserted data:")
    print(df)

if __name__ == "__main__":
    setup_clickhouse()
    #insert_sample_data()

