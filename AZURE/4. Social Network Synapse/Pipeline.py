import tweepy

# Authenticate with Twitter API
consumer_key = 'your_consumer_key'
consumer_secret = 'your_consumer_secret'
access_token = 'your_access_token'
access_token_secret = 'your_access_token_secret'

auth = tweepy.OAuth1UserHandler(consumer_key, consumer_secret, access_token, access_token_secret)
api = tweepy.API(auth)

# Make API request to fetch tweets
tweets = api.search(q='your_search_query', count=100)

"-----------------------------------------------------------------------------------------------------"

# Perform data cleaning, restructuring, or analysis as needed
# Example: Extract relevant information from tweets
cleaned_tweets = [{'id': tweet.id, 'text': tweet.text, 'user': tweet.user.screen_name} for tweet in tweets]

"-----------------------------------------------------------------------------------------------------"

import pyodbc

# Connect to Azure Synapse
conn_str = 'your_connection_string'
conn = pyodbc.connect(conn_str)

# Create cursor
cursor = conn.cursor()

# Iterate over cleaned tweets and insert into Azure Synapse table
for tweet in cleaned_tweets:
    cursor.execute("INSERT INTO your_table (tweet_id, tweet_text, user_name) VALUES (?, ?, ?)", 
                   (tweet['id'], tweet['text'], tweet['user']))

# Commit changes and close connection
conn.commit()
conn.close()
