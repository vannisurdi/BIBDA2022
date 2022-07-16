import tweepy
import numpy as np
import pandas as pd
import pprint
from tqdm import tqdm_notebook as tqdm 
import time
from datetime import datetime
import math

#----------------------------------------------------------------------
# CREA UN CLIENT AUTORIZZATO CON OAUTH2
#----------------------------------------------------------------------
consumer_key = '1530229487446769665-q73AeeKl20Y1ck8m8xtrlPEZqZTjeW' #access token
consumer_secret = 'Z94IlGGHcAxeFxkD' #access token secret
access_token = 'DJF1NBQJU7WkeA5kl26iRQyXx' #api key
access_token_secret = 'bUqdHHBlCzyiuVzoZS9ozIvcRq4kOTeyIYkaD2mRUOcZwg234y' # api key secret
bearer_token = 'AAAAAAAAAAAAAAAAAAAAAKs%2FdAEAAAAAGVOzXj3eyc%2BF%2FgD3CbIavSKGc6E%3Dbm6sKTR86aDnqKUTdfKX5VVrLEHJsNRaRs8tvRgtIuhToEFTNr' #bearer token
##############
# MARCELLO
##############
# consumer_key = '1530903206368362497-PS5RkaL0wq70lqzDs1z35YIPNNDfnB' #access token
# consumer_secret = 'Z94IlGGHcAxeFxkD' #access token secret
# access_token = 'tuqRIYb62IUzs1yQponckAyP2' #api key
# access_token_secret = 'sffLevfKpmFE77wQOqatB5FFbrQtBQVfqMvWRgZtVODCjZE6iY' # api key secret
# bearer_token = 'AAAAAAAAAAAAAAAAAAAAAHZ0eAEAAAAA%2BFeSDD7W%2BZqxWIneQQmPW%2BY6KPU%3DYhmRSLKnUzLxzUSwJxgtx0rveZwwXPiVOPytIEwf5uQUiH3ZlT' #bearer token

client = tweepy.Client( bearer_token=bearer_token, 
                        consumer_key=consumer_key,  
                        consumer_secret=consumer_secret, 
                        access_token=access_token, 
                        access_token_secret=access_token_secret, 
                        wait_on_rate_limit=True)

#---------------------------------------------------------------------- 
# ARRICCHISCO DATI DEI TWEET DA STREAMING
#----------------------------------------------------------------------
types = {'id': str, 'verified': str}
df_exclus_str = pd.read_csv('st_stream_uniques.csv', sep=';', dtype=types)
ids =list(df_exclus_str['id'])
iter_size   = 10
iter_end    = math.ceil(len(ids)/iter_size)
iter_count  = 798
first_file_write = False
api_errors = []

def extract_hash_tags(s):
    return set(part[1:] for part in s.split() if part.startswith('#'))

while iter_count < iter_end:
    x1 = iter_count * iter_size
    x2 = x1 + iter_size
    iter_ids = ids[x1:x2]
    
    total_data = []
    for id in iter_ids:
        print(id)
        tweets = client.get_tweet(id=id,                               
                                tweet_fields=['context_annotations', "id", "text", "source", 'created_at', 'author_id', 'public_metrics', 'geo'],
                                user_fields = ["name", "username", "location", "verified", "description"],
                                expansions='author_id')
        total_data.append(tweets)
        time.sleep(2)

    e_text = 'iter: ', iter_count
    print(e_text)
    api_errors.append(e_text)
    df_api_errors = pd.DataFrame(api_errors)
    df_api_errors.to_csv('augmentation_log.csv', mode='a', index=False, header=False, sep=';')
    api_errors = []

    tweet_info_ls = []
    user_info_ls  = []

    # iterate over each tweet and corresponding user details
    for t in total_data:
        try:
            tweet = t.data   
            #print(tweet) 
            tweet_info = {
                'id': tweet.id,
                'created_at': tweet.created_at,
                'text': tweet.text,
                'source': tweet.source,
                'author_id': tweet.author_id,
                'retweet_count': tweet.public_metrics["retweet_count"] if 'retweet_count' in tweet.public_metrics else 0,
                'like_count': tweet.public_metrics["like_count"],
                'hashtags': extract_hash_tags(tweet.text)
            }
            tweet_info_ls.append(tweet_info)
        except:
            e_text = 'error on t.data', str(id), 'iter: ', iter_count
            print(e_text)
            api_errors.append(e_text)
            df_api_errors = pd.DataFrame(api_errors)
            df_api_errors.to_csv('augmentation_log.csv', mode='a', index=False, header=False, sep=';')
            api_errors = []


    for t in total_data:
        try:
            users = t.includes["users"]
            for user in users:
                user_info = {
                    'author_id': user.id,
                    'name': user.name,
                    'username': user.username,
                    'location': user.location,
                    'verified': user.verified,
                    'user_description': user.description
                }
                user_info_ls.append(user_info)
        except:
            e_text = 'error on t.includes', str(id), 'iter: ', iter_count
            print(e_text)
            api_errors.append(e_text)
            df_api_errors = pd.DataFrame(api_errors)
            df_api_errors.to_csv('augmentation_log.csv', mode='a', index=False, header=False, sep=';')
            api_errors = []
            
    tweets_df = pd.DataFrame(tweet_info_ls)
    users_df = pd.DataFrame(user_info_ls)

    tweets_final_df = tweets_df.merge(users_df, on=["author_id"], how="left")

    tweets_final_df.to_csv('sc_stream_augmented_total.csv', mode='a', index=False, header=not(first_file_write), sep=';')
    first_file_write = True
    iter_count += 1