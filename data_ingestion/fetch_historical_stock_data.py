import yfinance as yf
import pandas as pd

# List of stock symbols
stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Define the start and end dates for historical data
start_date = '2020-01-01'
end_date = '2023-01-01'

# Fetch historical data for each stock
for stock in stocks:
    ticker = yf.Ticker(stock)
    hist_data = ticker.history(start=start_date, end=end_date)

    # Print some info
    print(f"Showing historical data for {stock}:")
    print(hist_data.head())

    # Test: save data to a CSV file
    hist_data.to_csv(f"{stock}_historical_data.csv")
