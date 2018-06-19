# -*- coding: utf-8 -*-
#Import the necessary methods from tweepy library
from __future__ import absolute_import, print_function

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler,API
from tweepy import Stream

import json
from kafka import SimpleProducer, KafkaClient

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize


#Variables that contains the user credentials to access Twitter API 
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""


#This is a basic listener that just prints received tweets to stdout.
class StdOutListener(StreamListener):

    def __init__(self,api):

        self.api = api
        super(StreamListener, self).__init__()
        client = KafkaClient("localhost:9092")
        self.producer = SimpleProducer(client, async = True, batch_send_every_n = 1000, batch_send_every_t = 20)

    def on_status(self,status):
        msg = status.text.encode("utf-8")
        #lines_list = tokenize.sent_tokenize(paragraph)
        
        try:
            self.producer.send_messages(b'twitterstream1',msg)
        except Exception as e:
            print(e)
            return False
        print("#####################################",status.text,status.coordinates, status.place)
        return True

    def on_error(self, status_code):
        print("Error received in kafka producer")
        return True
    def on_timeout(self):
        return True
if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    
    api = API(auth)
    l = StdOutListener(api)

    stream = Stream(auth, l)

    #This line filter Twitter Streams to capture data by the keywords: 'python', 'javascript', 'ruby'
    stream.filter(track=['#trump', '#obama'])

