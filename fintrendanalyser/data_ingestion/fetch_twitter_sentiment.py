"""
    This script is currently disabled due to Twitter endpoint limitations
"""
# import os
# import tweepy
# from dotenv import load_dotenv
# from textblob import TextBlob


# Load environment variables from .env file
# load_dotenv()

# # X API credentials
# api_key = os.getenv('X_API_KEY')
# api_secret = os.getenv('X_API_SECRET')
# access_token = os.getenv('X_ACCESS_TOKEN')
# access_token_secret = os.getenv('X_ACCESS_TOKEN_SECRET')

# # Authenticate with Twitter
# auth = tweepy.OAuthHandler(api_key, api_secret)
# auth.set_access_token(access_token, access_token_secret)
# api = tweepy.API(auth)

# # List of company names or relevant hashtags
# companies = ["Apple", "Microsoft", "Google", "Amazon", "Tesla"]

# # Number of tweets to fetch for each company
# tweet_count = 100

# # Fetch and analyze tweets
# for company in companies:
#     print(f"Fetching tweets for {company} and analyzing sentiment...")
#     tweets = api.search_tweets(q=company, count=tweet_count, lang="en")

#     sentiment_sum = 0
#     for tweet in tweets:
#         analysis = TextBlob(tweet.text)
#         sentiment_sum += analysis.sentiment.polarity

#     # Calculate average sentiment
#     avg_sentiment = sentiment_sum / len(tweets)
#     print(f"Average sentiment for {company}: {avg_sentiment}\n")
