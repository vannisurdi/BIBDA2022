import tweepy
import numpy as np
import pandas as pd
import pprint
from tqdm import tqdm_notebook as tqdm 
import time
from datetime import datetime

api_key = 'DJF1NBQJU7WkeA5kl26iRQyXx' #api key
api_key_secret = 'bUqdHHBlCzyiuVzoZS9ozIvcRq4kOTeyIYkaD2mRUOcZwg234y' # api key secret
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAKs%2FdAEAAAAAGVOzXj3eyc%2BF%2FgD3CbIavSKGc6E%3Dbm6sKTR86aDnqKUTdfKX5VVrLEHJsNRaRs8tvRgtIuhToEFTNr' #bearer token
access_token = '1530229487446769665-q73AeeKl20Y1ck8m8xtrlPEZqZTjeW' #access token
access_token_secret = 'Z94IlGGHcAxeFxkD' #access token secret

client = tweepy.Client(bearer_token, api_key, api_key_secret, access_token, access_token_secret)

auth = tweepy.OAuth1UserHandler(api_key, api_key_secret, access_token, access_token_secret)
api = tweepy.API(auth)

from tweepy.tweet import ReferencedTweet
search_terms = ["elezioniamministrative", "ballottaggio26giugno", "ballottaggi", "centrosinistra", "centrodestra"]  #["elezion", "amministrativ", "sboarina", "tommasi", "tosi", "votare", "voto", "centrosinistra", "centrodestra", "cinque stelle", "fratelli d'italia", "forza italia", "partito democratico", "catanzaro", "verona", "lucca", "parma", "piacenza", "viterbo", "frosinone", "alessandria", "cuneo", "monza", "como", "gorizia", "barletta"]

class my_stream(tweepy.StreamingClient):

  def on_connect(self):
    print("connected")
     
  def on_tweet(self, tweet):
      if tweet.referenced_tweets == None:
        now = datetime.now()
        # Open a file with access mode 'a'
        file_object = open('stream_26_06.txt', 'a')                
        file_object.write(str(tweet.data) + '\n' + '$800A$ ' + str(now) + '\n')
        file_object.close()
        print(tweet.data)
        time.sleep(0.2)

stream = my_stream(bearer_token=bearer_token)

for term in search_terms:
  stream.add_rules(tweepy.StreamRule(term))

#stream.filter(tweet_fields=["referenced_tweets"])
stream.filter()