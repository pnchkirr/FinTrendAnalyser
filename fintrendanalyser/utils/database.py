from dotenv import load_dotenv
from utils.logging import setup_logger
import os
import psycopg2


# Load environment variables from .env file
load_dotenv()

# PostgreSQL connection parameters
db_params = {
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
}

logger = setup_logger(name='database_management')

def get_db_connection():
    try:
        conn = psycopg2.connect(**db_params)
        logger.info('Successfully connected to the database.')
        return conn
    except Exception as e:
        logger.error('Database connection failed: ', exc_info=e)
        exit()


def create_table_with_schema(
        connection,
        db_schema,
        db_table_name,
        db_table_schema_definition,
        constraints=None
        ):
    """
    Create a table within a specific schema with the given schema definition
    and optional constraints, if it does not exist.

    Parameters:
        connection: Connection to the database.
        db_schema (str): Name of the schema where the table will be created.
        db_table_name (str): Name of the table to create.
        schema_definition (dict): Dictionary where keys are column names and values are data types.
        constraints (list of str, optional): List of SQL constraint definitions.
    """
    column_defs = ', '.join([f'{col} {col_type}' for col, col_type in db_table_schema_definition.items()])
    constraint_defs = ', '.join(constraints) if constraints else ''
    table_definition = f"{column_defs}, {constraint_defs}" if constraints else column_defs
    full_table_name = f"{db_schema}.{db_table_name}"
    sql = f'CREATE SCHEMA IF NOT EXISTS {db_schema}; CREATE TABLE IF NOT EXISTS {full_table_name} ({table_definition});'

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
        logger.info(f'Table "{full_table_name}" created or already exists.')
    except Exception as e:
        logger.error(f'Failed to create table "{full_table_name}": ', exc_info=e)


def insert_data(
        connection,
        db_schema,
        db_table_name,
        table_columns,
        table_data,
        on_conflict_action=None):
    """
    Insert data into a table within a specific schema, with an optional conflict handling.

    Parameters:
        connection: The database connection object.
        db_schema (str): Name of the schema where the table resides.
        db_table_name (str): The name of the table where data will be inserted.
        table_columns (list): The list of column names for the insert.
        table_data (tuple): The values to insert into the table.
        on_conflict_action (str, optional): SQL clause for conflict handling, e.g., 'ON CONFLICT (column) DO NOTHING'.
    """
    column_names = ', '.join(table_columns)
    placeholders = ', '.join(['%s'] * len(table_data))
    full_table_name = f'{db_schema}.{db_table_name}'
    sql = f'INSERT INTO {full_table_name} ({column_names}) VALUES ({placeholders})'

    if on_conflict_action:
        sql += f' {on_conflict_action}'

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, table_data)
            if cursor.rowcount > 0:
                # Only log if the row was actually inserted
                key_value_info = ", ".join([f"{col}={val}" for col, val in zip(table_columns, table_data)])
                logger.info(f'Data inserted into "{full_table_name}": {key_value_info}.')
            else:
                # Optionally log when data was not inserted due to conflict, or omit this part to reduce log noise
                # logger.info(f'No new data to insert for "{full_table_name}". Data might already exist or conflict.')
                pass
        connection.commit()
    except Exception as e:
        logger.error(f'Failed to insert data into "{full_table_name}": ', exc_info=e)


def execute_select(connection, query, params=()):
    """
    Execute a SELECT statement and return the fetched results.

    Parameters:
        connection: The database connection object.
        query (str): The SQL query to execute.
        params (tuple): Parameters for the SQL query.

    Returns:
        list: The fetched results.
    """
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        return cursor.fetchall()
