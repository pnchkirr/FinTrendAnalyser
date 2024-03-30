from utils.constants import (
    DB_PROCESSED_DATA_SCHEMA,
    DB_RAW_DATA_SCHEMA,
    FINANCIAL_NEWS_TABLE_NAME,
    FINANCIAL_NEWS_TABLE_SCHEMA
)
from utils.database import (
    get_db_connection,
    create_table_with_schema,
    execute_select,
    insert_data,
)
from utils.logging import setup_logger


logger = setup_logger(name='transform_financial_news')

def transform_financial_news():
    # Establish database connection
    conn = get_db_connection()

    create_table_with_schema(
        connection=conn,
        db_schema=DB_PROCESSED_DATA_SCHEMA,
        db_table_name=FINANCIAL_NEWS_TABLE_NAME,
        db_table_schema_definition=FINANCIAL_NEWS_TABLE_SCHEMA
    )

    # Retrieve raw data from the raw schema's table
    raw_table_fullname = f"{DB_RAW_DATA_SCHEMA}.{FINANCIAL_NEWS_TABLE_NAME}"
    raw_data = execute_select(conn, f"SELECT * FROM {raw_table_fullname}")

    # Process the data: remove entries with "[Removed]"
    processed_data = [
        row for row in raw_data if "[Removed]" not in row
    ]

    # Insert processed data into the processed schema's table
    for data_row in processed_data:
        insert_data(
            connection=conn,
            db_schema=DB_PROCESSED_DATA_SCHEMA,
            db_table_name=FINANCIAL_NEWS_TABLE_NAME,
            table_columns=list(FINANCIAL_NEWS_TABLE_SCHEMA.keys()),
            table_data=data_row
        )

    # Close the database connection
    conn.close()
    logger.info('Finished transforming and storing financial news data.')

if __name__ == "__main__":
    transform_financial_news()
