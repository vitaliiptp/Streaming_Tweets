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

language = "de"
end_date = "2020-08-30"
profile_country = "DE"

# parser=tweepy.parsers.JSONParser()

results = []

try:
    for tweet in tweepy.Cursor(api.search,
                                q="*",
                                lang=language,
                                until=end_date,
                                tweet_mode='extended',
                                exclude_replies=False,
                                include_retweets=True
                                ).items(3000):
        if tweet.retweet_count > 140:
           results.append(tweet)

except tweepy.TweepError:
    time.sleep(180)


def toDataFrame(tweets):
    DataSet = pd.DataFrame()

    DataSet['tweetID'] = [tweet.id for tweet in tweets]
    DataSet['tweetText'] = [tweet.full_text for tweet in tweets]
    DataSet['tweetRetweetCt'] = [tweet.retweet_count for tweet in tweets]
    DataSet['tweetFavoriteCt'] = [tweet.favorite_count for tweet in tweets]
    DataSet['tweetSource'] = [tweet.source for tweet in tweets]
    DataSet['tweetCreated'] = [tweet.created_at for tweet in tweets]
    DataSet['userID'] = [tweet.user.id for tweet in tweets]
    DataSet['userScreen'] = [tweet.user.screen_name for tweet in tweets]
    DataSet['userName'] = [tweet.user.name for tweet in tweets]
    DataSet['userCreateDt'] = [tweet.user.created_at for tweet in tweets]
    DataSet['userDesc'] = [tweet.user.description for tweet in tweets]
    DataSet['userFollowerCt'] = [tweet.user.followers_count for tweet in tweets]
    DataSet['userFriendsCt'] = [tweet.user.friends_count for tweet in tweets]
    DataSet['userLocation'] = [tweet.user.location for tweet in tweets]
    DataSet['userTimezone'] = [tweet.user.time_zone for tweet in tweets]
    DataSet['hashtags'] = [tweet.entities['hashtags'] for tweet in tweets]
    DataSet['statuses_count'] = [tweet.user.statuses_count for tweet in tweets]
    DataSet['location'] = [tweet.place for tweet in tweets]

    return DataSet


# Pass the tweets list to the above function to create a DataFrame
DataSet = toDataFrame(results)