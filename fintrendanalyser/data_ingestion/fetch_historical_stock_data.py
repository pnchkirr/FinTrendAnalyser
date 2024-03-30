from utils.constants import (
    DB_RAW_DATA_SCHEMA,
    HISTORICAL_STOCK_DATA_TABLE_NAME,
    HISTORICAL_STOCK_DATA_TABLE_SCHEMA,
    HISTORICAL_STOCK_DATA_TABLE_CONSTRAINTS
)
from utils.database import (
    get_db_connection,
    create_table_with_schema,
    insert_data,
    execute_select
)
from utils.logging import setup_logger
import datetime
import sys
import yfinance as yf


logger = setup_logger(name='fetch_historical_stock_data')

# Establish a database connection
conn = get_db_connection()

# Create table with the defined schema
create_table_with_schema(
    connection=conn,
    db_schema=DB_RAW_DATA_SCHEMA,
    db_table_name=HISTORICAL_STOCK_DATA_TABLE_NAME,
    db_table_schema_definition=HISTORICAL_STOCK_DATA_TABLE_SCHEMA,
    constraints=HISTORICAL_STOCK_DATA_TABLE_CONSTRAINTS
)

# List of stock symbols
stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Define the start and end dates for historical data
# start_date = '2020-01-01'
# end_date = '2023-01-01'

# Flag for full backfill
FULL_BACKFILL = '--full-backfill' in sys.argv

# Fetch historical data for each stock
for stock in stocks:
    ticker = yf.Ticker(stock)

    # Query the latest date in the database for the current stock
    latest_date_results = execute_select(conn, "SELECT MAX(datetime) FROM historical_stock_data WHERE symbol = %s", (stock,))
    latest_date = latest_date_results[0][0] if latest_date_results else None

    # Get the current date
    current_date = datetime.datetime.now().date()

    # If there's no data yet for this stock or the flag for full backfill is specified, fetch a longer history
    if FULL_BACKFILL or latest_date is None:
        logger.info(f"Fetching full history for {stock}.")
        hist_data = ticker.history(period="max")

        # Log the number of records fetched for full backfill
        num_records = len(hist_data)
        logger.info(f"Fetched {num_records} records for full backfill of {stock}.")

        if num_records == 0:
            logger.warning(f"No data available to insert for full backfill of {stock}.")

    elif latest_date.date() < current_date:
        # Fetch data from the day after the latest date
        start_date = latest_date.date() + datetime.timedelta(days=1)
        end_date = current_date + datetime.timedelta(days=1)

        logger.info(f"Fetching incremental data for {stock} from {start_date} to {end_date}.")
        hist_data = ticker.history(start=start_date, end=end_date)

        # Log the number of records fetched
        num_records = len(hist_data)
        logger.info(f"Fetched {num_records} records for {stock}.")
        if num_records == 0:
            logger.info(f"No new data available to insert for {stock}.")
    else:
        logger.info(f"No new data to fetch for {stock}.")
        continue

    # Insert data into the database
    for index, row in hist_data.iterrows():
        data_values = (
            stock,
            index.to_pydatetime(),
            row['Open'],
            row['High'],
            row['Low'],
            row['Close'],
            row['Volume']
        )
        insert_data(
            conn,
            db_schema=DB_RAW_DATA_SCHEMA,
            db_table_name=HISTORICAL_STOCK_DATA_TABLE_NAME,
            table_columns=list(HISTORICAL_STOCK_DATA_TABLE_SCHEMA.keys()),
            table_data=data_values
        )
    conn.commit()

    # Print some info
    # print(f"Showing fetched historical data for {stock}:")
    # print(hist_data.head())
    logger.info(f"Completed fetching data for {stock}.")

    # Test: save data to a CSV file
    # hist_data.to_csv(f"{stock}_historical_data.csv")

# Close the connection
conn.close()
logger.info("Database connection closed.")
