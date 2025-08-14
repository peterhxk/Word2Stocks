import os
import praw
import boto3
import json
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Reddit API credentials from .env
REDDIT_CLIENT_ID = os.getenv('REDDIT_CLIENT_ID')
REDDIT_CLIENT_SECRET = os.getenv('REDDIT_CLIENT_SECRET')
REDDIT_USER_AGENT = os.getenv('REDDIT_USER_AGENT')

# AWS Kinesis setup
AWS_REGION = os.getenv('AWS_DEFAULT_REGION')
KINESIS_STREAM_NAME = os.getenv('KINESIS_STREAM_NAME')
kinesis_client = boto3.client('kinesis', region_name=AWS_REGION)

# Initialize Reddit client
reddit = praw.Reddit(
	client_id=REDDIT_CLIENT_ID,
	client_secret=REDDIT_CLIENT_SECRET,
	user_agent=REDDIT_USER_AGENT
)

class RedditStreamListener:
    """
    Reddit stream listener that sends new posts to Kinesis Data Stream
    """

    def __init__(self, subreddit_names):
        self.subreddits = [reddit.subreddit(names) for names in subreddit_names.split(',')]

    def filter_posts(self, submission):
        """
        Filters posts based on keywords
        """
        if len(submission.title.split()) > 10:
            return
        
        rules = ["AAPL OR TSLA OR MSFT OR GOOGL OR AMZN",
                "stock market OR trading OR NYSE OR NASDAQ",
                "earnings OR quarterly report OR financial results"]
        normalized_post = submission.selftext.lower() + " " + submission.title.lower()
        for keyword in rules:
            if keyword in normalized_post:
                

    def stream_posts(self):
        subreddit = reddit.subreddit('+'.join(self.subreddits))
        for submission in subreddit.stream.submissions():
            

    