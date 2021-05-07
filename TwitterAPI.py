import json
import os
import jsonpickle as jsonpickle
import tweepy as tw
import pandas as pd
from time import sleep
from json import dumps
from kafka import KafkaProducer


consumer_key = '#############################'
consumer_secret = '################################'
access_token = '###################################################'
access_token_secret = '############################################'

auth = tw.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tw.API(auth, wait_on_rate_limit=True)


search_words = "#put your hashtag here"
print(search_words)
date_since = "put a date to start searching from it"


check_tweets = []
while True :
    tweets = tw.Cursor(api.search,
                       q=search_words,
                       since=date_since,lang="en").items()

    record = {}
    print("Loading...")
    for tweet in tweets:
        print(tweet.text)
        User_ID = tweet.user.id
        User_Name = tweet.user.name
        User_Screen_Name = tweet.user.screen_name
        Followers_Count = tweet.user.followers_count
        Friends_Count = tweet.user.friends_count
        Verified = tweet.user.verified
        Tweet_ID = tweet.id
        Tweet_Text = tweet.text
        Tweet_Date = tweet.created_at
        Source = tweet.source
        Retweet_Count = tweet.retweet_count
        Likes = tweet.favorite_count
        if Tweet_ID not in check_tweets:
            check_tweets.append(Tweet_ID)
            # print(check_tweets)
            # tweetdate = tweet.created_at
            record = {"User_ID":User_ID, "User_Name":User_Name, "User_Screen_Name":User_Screen_Name, "Followers_Count":Followers_Count, "Friends_Count":Friends_Count,
                      "Verified":Verified, "Tweet_ID":Tweet_ID, "Tweet_Text":Tweet_Text, "Tweet_Date":Tweet_Date, "Source":Source, "Retweet_Count":Retweet_Count, "Likes":Likes}
            row = jsonpickle.encode(record)
            # data = json.loads(str(tweet))
            producer = KafkaProducer(bootstrap_servers=['sandbox-hdp.hortonworks.com:6667'],
                                     value_serializer=lambda x:
                                     dumps(x).encode('utf-8'))

            producer.send('kafka topic name', value=row)

            sleep(5)




