DB_RAW_DATA_SCHEMA = 'raw'
DB_PROCESSED_DATA_SCHEMA = 'processed'

ECONOMIC_DATA_TABLE_NAME = 'economic_data'
ECONOMIC_DATA_TABLE_SCHEMA = {
    'indicator_id': 'VARCHAR(50)',
    'indicator_value': 'TEXT',
    'country_id': 'VARCHAR(50)',
    'country_value': 'TEXT',
    'countryiso3code': 'VARCHAR(3)',
    'date': 'VARCHAR(4)',
    'value': 'NUMERIC',
    'unit': 'VARCHAR(50)',
    'obs_status': 'VARCHAR(50)',
    'decimal': 'INT'
}

FINANCIAL_NEWS_TABLE_NAME = 'financial_news'
FINANCIAL_NEWS_TABLE_SCHEMA = {
    'company': 'VARCHAR(100)',
    'title': 'TEXT',
    'description': 'TEXT',
    'url': 'TEXT',
    'published_at': 'TIMESTAMP WITH TIME ZONE',
    'source_name': 'VARCHAR(100)'
}

HISTORICAL_STOCK_DATA_TABLE_NAME = 'historical_stock_data'
HISTORICAL_STOCK_DATA_TABLE_SCHEMA = {
    'symbol': 'VARCHAR(10)',
    'datetime': 'TIMESTAMP WITH TIME ZONE',
    'open': 'FLOAT',
    'high': 'FLOAT',
    'low': 'FLOAT',
    'close': 'FLOAT',
    'volume': 'BIGINT'
}
HISTORICAL_STOCK_DATA_TABLE_CONSTRAINTS = [
    'PRIMARY KEY (symbol, datetime)'
]

INTRADAY_STOCK_DATA_TABLE_NAME = 'intraday_stock_data'
INTRADAY_STOCK_DATA_TABLE_SCHEMA = {
    'symbol': 'VARCHAR(10)',
    'datetime': 'TIMESTAMP WITHOUT TIME ZONE',
    'open': 'FLOAT',
    'high': 'FLOAT',
    'low': 'FLOAT',
    'close': 'FLOAT',
    'volume': 'BIGINT'
}
INTRADAY_STOCK_DATA_TABLE_CONSTRAINTS = [
    'PRIMARY KEY (symbol, datetime)'
]

STOCK_SMA_DATA_TABLE_NAME = 'stock_sma_data'
STOCK_SMA_DATA_TABLE_SCHEMA = {
    'symbol': 'VARCHAR(10)',
    'datetime': 'DATE',
    'sma': 'FLOAT'
}
STOCK_SMA_DATA_TABLE_CONSTRAINTS = [
    'PRIMARY KEY (symbol, datetime)'
]
