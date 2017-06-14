#!/usr/bin/env python2.7
# -*- coding: utf-8 -*-

#Import the necessary methods from tweepy library

import time
from time import gmtime, strftime

import tweepy
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

import json
import pandas as pd
import matplotlib.pyplot as plt

# My credentials to access Twitter API
#-------------------------------------

Consum_key = "put your key here"
Consum_secret = "put your secret here"
Access_token = "access token" 
Access_secret = "access token"
#--------------------------------------


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    #def on_data(self, data):
    #   print data
    #   return True

    #def on_error(self, status):
    #    print status
    def on_data(self, data):
        #print data
        tweeting = data.split(',"text":"')[1].split('","source')[0]
        print tweeting
        tweeting_time = strftime("%a, %d %b %Y %H:%M:%S", gmtime())
        Tweet =  tweeting_time+"-->"+tweeting
        #Tweet = str(time.time())+"-->"+tweeting
        #print Tweet
        Saving =  open('Twitter_Stream.csv', 'a')
        Saving.write(data) #Saving.write(Tweet) put tweet in the file later
        Saving.write('\n')
        Saving.close()
        return True

    def json_render(self, tweets):
        tweets_data = []
        tweets_file = open('Twitter_Stream.csv', 'r')
        for line in tweets_file:
            try:
                tweet = json.loads(line)
                tweets_data.append(tweet)
                print tweet
            except:
                continue

    def on_error(self, status):
        print 'Failed to proceed!',  str(e)
        time.sleep(1)

if __name__ == '__main__':
    Auth =  OAuthHandler(Consum_key, Consum_secret)
    Auth.set_access_token(Access_token, Access_secret)

    Twitterstream = Stream(Auth, StdOutListener())
    Twitterstream.filter(track =['japanese', 'songs', 'jpop'])
    Twitterstream.json_render

    #This handles Twitter authetification and the connection to Twitter Streaming API
    #l = StdOutListener()
    #auth = OAuthHandler(consumer_key, consumer_secret)
    #auth.set_access_token(access_token, access_token_secret)
    #stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    #stream.filter(track=['python', 'javascript', 'ruby'])
    

    #tweets_data_path = '../data/twitter_data.txt'
    #tweets_data = []
    #tweets_file = open('Twitter_Stream.csv', "r")
    #for line in tweets_file:
    #try:
    #   tweet = json.loads(line)
    #   tweets_data.append(tweet)
    # except:
    #   continue
    #print len(tweets_data)
    #tweets = pd.DataFrame()
    #tweets['text'] = map(lambda tweet: tweet['text'], tweets_data)
    #tweets['lang'] = map(lambda tweet: tweet['lang'], tweets_data)
    #tweets['country'] = map(lambda tweet: tweet['place']['country'] if tweet['place'] != None else None, tweets_data)

    #tweets_by_lang = tweets['lang'].value_counts()

    #fig, ax = plt.subplots()
    #ax.tick_params(axis='x', labelsize=15)
    #ax.tick_params(axis='y', labelsize=10)
    #ax.set_xlabel('Languages', fontsize=15)
    #ax.set_ylabel('Number of tweets' , fontsize=15)
    #ax.set_title('Top 5 languages', fontsize=15, fontweight='bold')
    #tweets_by_lang[:5].plot(ax=ax, kind='bar', color='red')
