import requests
import json

# My NewsAPI API Key
api_key = ''

# List of companies to fetch news for
companies = ['Apple', 'Microsoft', 'Google', 'Amazon', 'Tesla']

# Base URL for NewsAPI
base_url = "https://newsapi.org/v2/everything?"

# Fetch news for each company
for company in companies:
    # Build the query URL
    query_url = f"{base_url}q={company}&apiKey={api_key}"

    # Make the API request
    response = requests.get(query_url)

    # Check if the request was successful
    if response.status_code == 200:
        # Parse the response
        articles = response.json()['articles']

        # Print some info (or save it)
        print(f"Recent news for {company}:")
        for article in articles[:5]:  # to display the first 5 articles
            print(f"- {article['title']}")
    else:
        print(f"Failed to fetch news for {company}")

    print("\n")
