from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import json
import boto3
import time
import twitter_credentials as keys
import aws_s3_credentials as aws_keys


# This is a basic listener that just prints received tweets to stdout.
class StdOutListener (StreamListener):

    def on_data(self, data):
        tweet = json.loads (data)
        try:
            if 'extended_tweet' in tweet.keys ():
                # print (tweet['text'])
                tweet_details = [str (tweet['id']),
                               str (tweet['user']['name']),
                               str (tweet['user']['screen_name']),
                               tweet['extended_tweet']['full_text'],
                               str (tweet['user']['followers_count']),
                               str (tweet['user']['location']),
                               str (tweet['geo']),
                               str (tweet['created_at']),
                               '\n'
                               ]
                message = '\t'.join (tweet_details)
                print (message)
                client.put_record (
                    DeliveryStreamName=delivery_stream,
                    Record={
                        'Data': message
                    }
                )
            elif 'text' in tweet.keys ():
                # print (tweet['text'])
                tweet_details = [str (tweet['id']),
                               str (tweet['user']['name']),
                               str (tweet['user']['screen_name']),
                               tweet['text'].replace ('\n', ' ').replace ('\r', ' '),
                               str (tweet['user']['followers_count']),
                               str (tweet['user']['location']),
                               str (tweet['geo']),
                               str (tweet['created_at']),
                               '\n'
                               ]
                message = '\t'.join (tweet_details)
                print (message)
                client.put_record (
                    DeliveryStreamName=delivery_stream,
                    Record={
                        'Data': message
                    }
                )
        except (AttributeError, Exception) as e:
            print (e)
        return True

    def on_error(self, status):
        print (status)


if __name__ == '__main__':

    # This handles Twitter authetification and the connection to Twitter Streaming API
    listener = StdOutListener ()
    auth = OAuthHandler (keys.API_key, keys.API_key_secret)
    auth.set_access_token (keys.Access_token, keys.Access_token_secret)

    # tweets = Table('tweets_ft',connection=conn)
    client = boto3.client ('firehose',
                           region_name='eu-central-1',
                           aws_access_key_id=aws_keys.access_key_id,
                           aws_secret_access_key=aws_keys.secret_access_key
                           )

    delivery_stream = 'twitter_data_stream'
    # This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    # stream.filter(track=['trump'], stall_warnings=True)
    while True:
        try:
            print ('Twitter streaming...')
            stream = Stream (auth, listener)
            stream.filter (track=['AirPods Max'], languages=['en'], stall_warnings=True)
        except Exception as e:
            print (e)
            print ('Disconnected...')
            time.sleep (5)
            continue