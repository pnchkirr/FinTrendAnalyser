"""
    This script is currently not being used
"""
# from dotenv import load_dotenv
# from sec_edgar_downloader import Downloader
# import os


# Load environment variables from .env file
# load_dotenv()

# # Directory to save the filings
# # download_dir = "sec_filings"

# # Your email address (required by the SEC EDGAR system)
# email_address = os.getenv('SEC_FILINGS_EMAIL')

# # Create the directory if it doesn't exist
# os.makedirs(download_dir, exist_ok=True)

# # Initialize the downloader
# dl = Downloader(download_dir, email_address)

# # List of company tickers
# companies = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']

# # Define the type of filings and date range
# filing_type = "10-K"  # Annual reports; you can change this to "10-Q" for quarterly reports, etc.
# before_date = "2021-01-01"  # YYYY-MM-DD format

# # Download filings for each company
# for company in companies:
#     dl.get(filing_type, company, after=before_date)

#     print(f"Downloaded {filing_type} filings for {company}")
