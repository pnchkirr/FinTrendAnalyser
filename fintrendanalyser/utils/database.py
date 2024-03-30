from utils.logging import setup_logger
import psycopg2


# PostgreSQL connection parameters
DB_PARAMS = {
    'host': 'localhost',
    'port': '5433',
    'database': 'fintrendanalyzer',
    'user': 'postgres',
    'password': 'Jpoy8ethqw'
}

logger = setup_logger(name='database_management')

def get_db_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        logger.info('Successfully connected to the database.')
        return conn
    except Exception as e:
        logger.error('Database connection failed: ', exc_info=e)
        exit()


def create_table_with_schema(connection, table_name, schema, constraints=None):
    """
    Create a table with the given schema and optional constraints, if it does not exist.

    Parameters:
        conn (connection): Connection to the database.
        table_name (str): Name of the table to create.
        schema (dict): Dictionary where keys are column names and values are data types.
        constraints (list of str, optional): List of SQL constraint definitions.
    """
    column_defs = ', '.join([f'{col} {col_type}' for col, col_type in schema.items()])
    constraint_defs = ', '.join(constraints) if constraints else ''
    table_definition = f"{column_defs}, {constraint_defs}" if constraints else column_defs
    sql = f'CREATE TABLE IF NOT EXISTS {table_name} ({table_definition});'

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
        logger.info(f'Table "{table_name}" created or already exists.')
    except Exception as e:
        logger.error(f'Failed to create table "{table_name}": ', exc_info=e)


def insert_data(connection, table_name, columns, data, on_conflict_action=None):
    """
    Insert data into a table, with an optional conflict handling.

    Parameters:
        connection: The database connection object.
        table_name (str): The name of the table where data will be inserted.
        columns (list): The list of column names for the insert.
        data (tuple): The values to insert into the table.
        on_conflict_action (str, optional): SQL clause for conflict handling, e.g. 'ON CONFLICT (column) DO NOTHING'.

    """
    column_names = ', '.join(columns)
    placeholders = ', '.join(['%s'] * len(data))
    sql = f'INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})'

    if on_conflict_action:
        sql += f' {on_conflict_action}'

    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, data)
            if cursor.rowcount > 0:
                # Only log if the row was actually inserted
                key_value_info = ", ".join([f"{col}={val}" for col, val in zip(columns, data)])
                logger.info(f'Data inserted into "{table_name}": {key_value_info}.')
            else:
                # Optionally log when data was not inserted due to conflict, or omit this part to reduce log noise
                # logger.info(f'Data not inserted due to conflict for "{table_name}".')
                pass
        connection.commit()
    except Exception as e:
        logger.error(f'Failed to insert data into "{table_name}": ', exc_info=e)


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
