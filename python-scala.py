#!/usr/bin/env python
# coding: utf-8

# In[2]:


get_ipython().system('pip install tweepy')


# In[3]:


import tweepy 
from tweepy import OAuthHandler # to authenticate Twitter API
from tweepy import Stream 
from tweepy.streaming import StreamListener
import socket 
import json 


# Twitter developer Credentials to connect to twitter account
access_token = "****"
access_secret = "****"
consumer_key = "****"
consumer_secret = "****"


class TweetsListener(StreamListener):
    def __init__(self, csocket):
        self.client_socket = csocket

    def on_data(self, data):
        try:
            msg = json.loads(data)
            print(msg['text'].encode('utf-8'))
            self.client_socket.send(msg['text'].encode('utf-8'))
            return True

        except BaseException as e:
            print("Error: %s" % str(e))
            return True

    def on_error(self, status):
        print(status)
        return True


def sendData(c_socket):
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    twitter_stream = Stream(auth, TweetsListener(c_socket))
    twitter_stream.filter(track=['covid'])


# In[ ]:


s = socket.socket()
host = "127.0.0.1"
port = ****

s.bind((host, port))
print("Listening on port: %s" % str(port))

s.listen(5)
c, addr = s.accept()

print("Received request from: " + str(addr))

sendData(c)


# In[ ]:




