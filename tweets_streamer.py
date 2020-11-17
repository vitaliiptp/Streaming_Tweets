import tweepy
import csv
from tweepy.parsers import JSONParser
import json
import datetime
import pandas as pd
import twitter_credentials as keys
import time

auth = tweepy.auth.OAuthHandler(keys.API_key, keys.API_key_secret)
auth.set_access_token(keys.Access_token, keys.Access_token_secret)
api = tweepy.API(auth, wait_on_rate_limit=False, wait_on_rate_limit_notify=True)
