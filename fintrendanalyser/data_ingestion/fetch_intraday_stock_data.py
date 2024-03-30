from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
from dotenv import load_dotenv
from utils.constants import (
    INTRADAY_STOCK_DATA_TABLE_NAME,
    INTRADAY_STOCK_DATA_TABLE_SCHEMA,
    INTRADAY_STOCK_DATA_TABLE_CONSTRAINTS,
    STOCK_SMA_DATA_TABLE_NAME,
    STOCK_SMA_DATA_TABLE_SCHEMA,
    STOCK_SMA_DATA_TABLE_CONSTRAINTS
)
from utils.database import (
    get_db_connection,
    create_table_with_schema,
    insert_data,
    execute_select
)
from utils.logging import setup_logger
import os
import sys


# Load environment variables from .env file
load_dotenv()

logger = setup_logger(name='fetch_intraday_stock_and_sma_data')

# Establish a database connection
conn = get_db_connection()

# Create tables with the defined schemas
create_table_with_schema(
    connection=conn,
    table_name=INTRADAY_STOCK_DATA_TABLE_NAME,
    schema=INTRADAY_STOCK_DATA_TABLE_SCHEMA,
    constraints=INTRADAY_STOCK_DATA_TABLE_CONSTRAINTS
)

create_table_with_schema(
    connection=conn,
    table_name=STOCK_SMA_DATA_TABLE_NAME,
    schema=STOCK_SMA_DATA_TABLE_SCHEMA,
    constraints=STOCK_SMA_DATA_TABLE_CONSTRAINTS
)

api_key = os.getenv('ALPHA_VANTAGE_API_KEY')

# List of stock symbols
stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Flag for full backfill
FULL_BACKFILL = '--full-backfill' in sys.argv

# Initialize TimeSeries and TechIndicators class
ts = TimeSeries(key=api_key, output_format='pandas')
ti = TechIndicators(key=api_key, output_format='pandas')

for symbol in stocks:
    if FULL_BACKFILL:
        logger.info(f"Performing full backfill for intraday data of {symbol}.")
        intraday_data, _ = ts.get_intraday(symbol=symbol, interval='1min', outputsize='full')
    else:
        logger.info(f"Fetching incremental intraday data for {symbol}.")
        last_datetime_results = execute_select(conn, "SELECT MAX(datetime) FROM intraday_stock_data WHERE symbol = %s", (symbol,))
        last_datetime = last_datetime_results[0][0] if last_datetime_results else None
        outputsize = 'compact' if last_datetime is not None else 'full'
        intraday_data, _ = ts.get_intraday(symbol=symbol, interval='1min', outputsize=outputsize)

    # Insert intraday data
    for index, row in intraday_data.iterrows():
        data_values = (
            symbol,
            index.to_pydatetime(),
            row['1. open'],
            row['2. high'],
            row['3. low'],
            row['4. close'],
            row['5. volume']
        )
        insert_data(
            connection=conn,
            table_name=INTRADAY_STOCK_DATA_TABLE_NAME,
            columns=['symbol', 'datetime', 'open', 'high', 'low', 'close', 'volume'],
            data=data_values,
            on_conflict_action='ON CONFLICT (symbol, datetime) DO NOTHING'
        )

    try:
        sma_data, _ = ti.get_sma(symbol=symbol, interval='daily', time_period=20)
        # Insert SMA data
        for index, row in sma_data.iterrows():
            data_values = (
                symbol,
                index.date(),
                row['SMA']
            )
            insert_data(
                connection=conn,
                table_name=STOCK_SMA_DATA_TABLE_NAME,
                columns=['symbol', 'datetime', 'sma'],
                data=data_values,
                on_conflict_action='ON CONFLICT (symbol, datetime) DO NOTHING'
            )
    except ValueError as e:
        logger.error(f"API rate limit hit when fetching SMA data for {symbol}: {e}")
        # Optionally wait and retry or just log and continue/stop
        # time.sleep(60)  # Wait for a minute (adjust as needed)
        continue  # Skip to the next symbol or use break to stop the loop

    logger.info(f"Intraday and SMA data inserted for {symbol}")

    # Plotting (for testing)
    # intraday_data['4. close'].plot()
    # plt.title(f'Intraday TimeSeries for {symbol}')
    # plt.show()

# Close the database connection
conn.close()
