from alpha_vantage.timeseries import TimeSeries
from alpha_vantage.techindicators import TechIndicators
import matplotlib.pyplot as plt

# My AlphaVantage API Key
api_key = ''

# List of stock symbols
stocks = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# Initialize TimeSeries and TechIndicators class
ts = TimeSeries(key=api_key, output_format='pandas')
ti = TechIndicators(key=api_key, output_format='pandas')

for symbol in stocks:
    # Fetch intraday data
    data, _ = ts.get_intraday(symbol=symbol, interval='1min', outputsize='compact')
    print(f"Intraday data for {symbol}:")
    print(data.head())

    # Fetch a technical indicator example (e.g. SMA)
    sma_data, _ = ti.get_sma(symbol=symbol, interval='daily', time_period=20)
    print(f"SMA data for {symbol}:")
    print(sma_data.head())

    # Plotting (for testing)
    data['4. close'].plot()
    plt.title(f'Intraday TimeSeries for {symbol}')
    plt.show()
