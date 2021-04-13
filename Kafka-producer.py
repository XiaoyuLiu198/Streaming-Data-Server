#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from kafka import KafkaProducer
import json

access_token = "--"
access_token_secret =  "--"
api_key =  "--"
api_secret =  "--"
topic_name = "demo-3-twitter"

class StdOutListener(StreamListener):
    def on_data(self, data):
        json_ = json.loads(data) 
        producer.send(topic_name, json_.encode('utf-8'))
        return True
    def on_error(self, status):
        print (status)
        
if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    l = StdOutListener()
    auth = OAuthHandler(api_key, api_secret)
    auth.set_access_token(access_token, access_token_secret)
    stream = Stream(auth, l)
    stream.filter(track=["a", "the", "i", "you", "u"], languages= ["en"])

