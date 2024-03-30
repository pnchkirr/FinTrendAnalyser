from dotenv import load_dotenv
from utils.constants import (
    FINANCIAL_NEWS_TABLE_SCHEMA,
    FINANCIAL_NEWS_TABLE_NAME
)
from utils.database import (
    get_db_connection,
    create_table_with_schema,
    insert_data
)
from utils.logging import setup_logger
import os
import requests


# Load environment variables from .env file
load_dotenv()

logger = setup_logger(name='fetch_financial_news')

# Establish a database connection
conn = get_db_connection()

# Create table with the defined schema
create_table_with_schema(
    connection=conn,
    table_name=FINANCIAL_NEWS_TABLE_NAME,
    schema=FINANCIAL_NEWS_TABLE_SCHEMA
)

api_key = os.getenv('NEWS_API_KEY')

# List of companies to fetch news for
companies = ['Apple', 'Microsoft', 'Google', 'Amazon', 'Tesla']

# Base URL for NewsAPI
base_url = "https://newsapi.org/v2/everything?"

# Fetch news for each company
for company in companies:
    query_url = f"{base_url}q={company}&apiKey={api_key}"

    # Make the API request
    response = requests.get(query_url)

    if response.status_code == 200:
        # Parse the response
        articles = response.json()['articles']
        logger.info(f"Fetched {len(articles)} articles for {company}.")

        # Insert each article into the database
        for article in articles:
            data_values = (
                company,
                article['title'],
                article['description'],
                article['url'],
                article['publishedAt'],
                article['source']['name']
            )
            insert_data(
                connection=conn,
                table_name=FINANCIAL_NEWS_TABLE_NAME,
                columns=list(FINANCIAL_NEWS_TABLE_SCHEMA.keys()),
                data=data_values
            )
        logger.info(f"Finished fetching and inserting articles for {company}.")
    else:
        logger.error(f"Failed to fetch news for {company} with status code {response.status_code}.")

# Close the database connection
conn.close()
logger.info("Database connection closed.")
