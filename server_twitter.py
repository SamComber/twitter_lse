# load dependencies
import tweepy
import dataset
import json
from sqlalchemy.exc import ProgrammingError
import textblob
import time
import numpy as np
import pandas as pd
import sqlite3
import os

# create sqlite db if none exists, else append
db = dataset.connect("sqlite:///tweets_lse.db")

# set api constants
TWITTER_APP_KEY = 'pfekrUmzHx0WY4gNjQaIsjmjH'
TWITTER_APP_SECRET = 'OkZxpDJIMEtttGnUWigqhGvK4WqNnZLDLg79codmz56JZBhO9Q'
TWITTER_KEY = '1439343834-7xzhsIyOJYETIwiMaaZXj3qm1gQq0bge1QwgwRy'
TWITTER_SECRET = 'wjyIx0p1l7P2OB9jarfmRR500yzn7MOogJiSA9HEAh4qz'

# set OAuth authentication
auth = tweepy.OAuthHandler(TWITTER_APP_KEY, TWITTER_APP_SECRET)
auth.set_access_token(TWITTER_KEY, TWITTER_SECRET)

# create API object to pull twitter data, passing in authentication
api = tweepy.API(auth, 
                 retry_count = 5, # default number of retries to attempt when error occurs
                 retry_delay = 5, # number of seconds to wait between retries
                 wait_on_rate_limit = True, # Whether or not to automatically wait for rate limits to replenish
                 wait_on_rate_limit_notify = True) # Whether or not to print a notification when Tweepy is waiting for rate limits to replenish

# create class that inherits from streamlistener object
class StreamListener(tweepy.StreamListener):
    
    # override on_status method to define our own functionality     
    def on_status(self, status):
        
        # print all tweets that are not retweets         
        if hasattr(status, 'retweeted_status'):
            return
        
        description = status.user.description
        loc = status.user.location
        text = status.text
        place = status.place.full_name
        coords = status.coordinates
        name = status.user.screen_name
        blob = textblob.TextBlob(text)
        sentiment = blob.sentiment
        
        # only return geotagged tweets         
        if coords is None:
            return
            
        # encode to json         
        if coords is not None:
            coords = json.dumps(coords)
            coords = json.loads(coords)            
            lon = coords['coordinates'][0]
            lat = coords['coordinates'][1]
        
        # create reference point for db         
        table = db["tweets"]
        
        # try to insert to sqlite db         
        try:
            table.insert(dict(
                user_description=description,
                user_location=loc,
                place=place,
                lat=lat,
                lon=lon,
                text=text,
                user_name=name,
                polarity = sentiment.polarity,
                subjectivity = sentiment.subjectivity
            ))
        # catch any error          
        except ProgrammingError as err:
            logging.INFO(err)

    def on_error(self, status_code):
        # if being rate limited, return false         
        if status_code == 420:
            return False
        
def timeout_test(func):
    
    def wrapper(*args):
        t = time.clock()
        res = func(*args)
        print(func.__name__, time.clock() - t)
        return res
    
    return wrapper

@timeout_test # scrap = timer(call_scraper)
def call_scraper(geo):
    # instantiate streamlistener object from class
    stream_listener = StreamListener()

    # connect to twitter API to create stream object
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)

    # apply custom filtering
    stream.filter(locations=geo, languages=['en'])


# In[ ]:


import logging

# set logger
logger = logging.getLogger()
fhandler = logging.FileHandler(filename='twitter_log.log', mode='a')
formatter = logging.Formatter('%(asctime)s %(message)s')
fhandler.setFormatter(formatter)
logger.addHandler(fhandler)
logger.setLevel(logging.DEBUG)

# set geo
bbox_geo = [-10.8544921875, 49.82380908513249, 2.021484375, 59.478568831926395]

# call function with decorator method
call_scraper(bbox_geo)