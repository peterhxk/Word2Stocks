import tweepy
import json
import os
import time
from datetime import datetime
from dotenv import load_dotenv
from kinesis_client import put_record

load_dotenv()

class TwitterStreamListener(tweepy.StreamingClient):
    """
    Custom Twitter stream listener that sends tweets to Kinesis Data Stream
    """
    
    def on_connect(self):
        print("Connected to Twitter Stream")
    
    def on_tweet(self, tweet):
        """
        Process incoming tweets and send to Kinesis
        """
        try:
            # Prepare tweet data
            tweet_data = {
                'id': str(tweet.id),
                'text': tweet.text,
                'created_at': tweet.created_at.isoformat() if tweet.created_at else datetime.now().isoformat(),
                'author_id': str(tweet.author_id) if tweet.author_id else 'unknown',
                'public_metrics': tweet.public_metrics if hasattr(tweet, 'public_metrics') else {},
                'source': 'twitter',
                'timestamp': datetime.now().isoformat()
            }
            
            # Send to Kinesis using author_id as partition key for even distribution
            partition_key = str(tweet.author_id) if tweet.author_id else f"tweet_{tweet.id}"
            put_record(tweet_data, partition_key)
            
            print(f"Sent tweet {tweet.id} to Kinesis")
            
        except Exception as e:
            print(f"Error processing tweet {tweet.id}: {e}")
    
    def on_error(self, status_code):
        print(f"Stream error: {status_code}")
        if status_code == 420:
            # Rate limit exceeded
            print("Rate limit exceeded. Waiting...")
            time.sleep(60)
        return True  # Continue streaming
    
    def on_connection_error(self):
        print("Connection error occurred")
    
    def on_request_error(self, status_code):
        print(f"Request error: {status_code}")

class TwitterPoller:
    """
    Twitter API client for both streaming and polling
    """
    
    def __init__(self):
        self.bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
        if not self.bearer_token:
            raise ValueError("TWITTER_BEARER_TOKEN not found in environment variables")
        
        self.client = tweepy.Client(bearer_token=self.bearer_token)
        self.stream_client = TwitterStreamListener(self.bearer_token)
    
    def setup_stream_rules(self, rules):
        """
        Set up filtering rules for the Twitter stream
        
        Args:
            rules (list): List of rule strings, e.g., ["AAPL OR TSLA", "stock market"]
        """
        try:
            # Delete existing rules
            existing_rules = self.stream_client.get_rules()
            if existing_rules.data:
                rule_ids = [rule.id for rule in existing_rules.data]
                self.stream_client.delete_rules(rule_ids)
                print(f"Deleted {len(rule_ids)} existing rules")
            
            # Add new rules
            new_rules = [tweepy.StreamRule(rule) for rule in rules]
            self.stream_client.add_rules(new_rules)
            print(f"Added {len(new_rules)} new rules: {rules}")
            
        except Exception as e:
            print(f"Error setting up stream rules: {e}")
    
    def start_streaming(self, stock_symbols=None):
        """
        Start streaming tweets based on stock symbols or general financial terms
        
        Args:
            stock_symbols (list): List of stock symbols to track, e.g., ["AAPL", "TSLA", "MSFT"]
        """
        if stock_symbols:
            # Create rules for specific stock symbols (without $ since it's not available in free tier)
            symbol_rules = [f"{symbol}" for symbol in stock_symbols]
            rules = [" OR ".join(symbol_rules)]
        else:
            # Default rules for general financial/stock market content
            rules = [
                "AAPL OR TSLA OR MSFT OR GOOGL OR AMZN",
                "stock market OR trading OR NYSE OR NASDAQ",
                "earnings OR quarterly report OR financial results"
            ]
        
        print(f"Setting up Twitter stream with rules: {rules}")
        self.setup_stream_rules(rules)
        
        # Start streaming
        print("Starting Twitter stream...")
        self.stream_client.filter(
            tweet_fields=["created_at", "author_id", "public_metrics", "context_annotations"]
        )
    
    def poll_recent_tweets(self, query, max_results=10):
        """
        Poll for recent tweets using search (alternative to streaming)
        
        Args:
            query (str): Search query
            max_results (int): Maximum number of tweets to retrieve
        """
        try:
            print(f"Searching for tweets with query: '{query}'")
            tweets = self.client.search_recent_tweets(
                query=query,
                max_results=max_results,
                tweet_fields=["created_at", "author_id", "public_metrics"]
            )
            
            if tweets.data:
                for tweet in tweets.data:
                    tweet_data = {
                        'id': str(tweet.id),
                        'text': tweet.text,
                        'created_at': tweet.created_at.isoformat(),
                        'author_id': str(tweet.author_id),
                        'public_metrics': tweet.public_metrics if hasattr(tweet, 'public_metrics') else {},
                        'source': 'twitter_search',
                        'timestamp': datetime.now().isoformat()
                    }
                    
                    partition_key = str(tweet.author_id)
                    put_record(tweet_data, partition_key)
                    print(f"Sent tweet {tweet.id} to Kinesis")
                    
            print(f"Processed {len(tweets.data) if tweets.data else 0} tweets")
            
        except tweepy.TooManyRequests:
            print("Rate limit exceeded. You've hit the Twitter API rate limit.")
            print("Free tier allows 300 requests per 15 minutes for search.")
            print("Wait 15 minutes before trying again, or consider upgrading to a paid tier.")
        except tweepy.Unauthorized:
            print("Unauthorized: Check your Bearer Token in the .env file")
        except tweepy.Forbidden:
            print("Forbidden: Your app may not have the required permissions")
        except Exception as e:
            print(f"Error polling tweets: {e}")

if __name__ == "__main__":
    try:
        twitter_poller = TwitterPoller()
        
        # Option 1: Start streaming (recommended for real-time data)
        # Uncomment the line below to start streaming
        # twitter_poller.start_streaming(["AAPL", "TSLA", "MSFT", "GOOGL", "AMZN"])
        
        # Option 2: Poll recent tweets (for testing or batch processing)
        print("Polling recent tweets...")
        twitter_poller.poll_recent_tweets("Apple", max_results=10)
        
    except KeyboardInterrupt:
        print("\nStopping Twitter polling...")
    except Exception as e:
        print(f"Error: {e}")
