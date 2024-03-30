from utils.constants import (
    DB_RAW_DATA_SCHEMA,
    ECONOMIC_DATA_TABLE_SCHEMA,
    ECONOMIC_DATA_TABLE_NAME
)
from utils.database import (
    get_db_connection,
    create_table_with_schema,
    insert_data
)
from utils.logging import setup_logger
import requests


logger = setup_logger(name='fetch_economic_data')

# Establish a database connection
conn = get_db_connection()

# Create table with the defined schema
create_table_with_schema(
    connection=conn,
    db_schema=DB_RAW_DATA_SCHEMA,
    db_table_name=ECONOMIC_DATA_TABLE_NAME,
    db_table_schema_definition=ECONOMIC_DATA_TABLE_SCHEMA
)

# World Bank API endpoint and query parameters
base_url = 'http://api.worldbank.org/v2/country/'
indicator_code = 'NY.GDP.MKTP.CD'  # GDP
country_code = 'USA'  # United States
date_range = '2010:2020'
url = f'{base_url}{country_code}/indicator/{indicator_code}?date={date_range}&format=json'

logger.info(f'Starting to fetch economic data from the World Bank API for {country_code}.')

# Make the request to the World Bank API
response = requests.get(url)

if response.status_code == 200:
    # Process the data
    data = response.json()[1]  # The actual data is in the second element of the response
    logger.info(f'Fetched {len(data)} records from the World Bank API for {country_code}.')
    for item in data:
        data_values = (
            item['indicator']['id'],
            item['indicator']['value'],
            item['country']['id'],
            item['country']['value'],
            item['countryiso3code'],
            item['date'],
            item.get('value', None),
            '',
            '',
            item['decimal']
        )
        insert_data(
            connection=conn,
            db_schema=DB_RAW_DATA_SCHEMA,
            db_table_name=ECONOMIC_DATA_TABLE_NAME,
            table_columns=list(ECONOMIC_DATA_TABLE_SCHEMA.keys()),
            table_data=data_values
        )
    logger.info(f'Finished fetching and inserting economic data for {country_code}.')
else:
    logger.error(f'Failed to fetch economic data for {country_code} with status code {response.status_code}.')

# Close the database connection
conn.close()
logger.info('Database connection closed.')
