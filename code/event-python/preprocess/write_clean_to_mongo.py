import pymongo as pm
import sys
import json


def get_tweet_collection():
    conn = pm.MongoClient('dmserv4.cs.illinois.edu', 11111)
    tweet_collection = conn['tweet_db']['nyc_tweets']
    return tweet_collection


def write_tweets(tweet_collection, output_file):
    with open(output_file, 'w') as fout:
        for tweet in tweet_collection.find():
            clean_tweet = extract_clean_tweet(tweet)
            fout.write(json.dumps(clean_tweet, sort_keys=True) + '\n')


def extract_clean_tweet(tweet):
    clean_tweet = {}
    clean_tweet['id'] = tweet['id']
    clean_tweet['text'] = tweet['text']
    clean_tweet['created_at'] = tweet['created_at']
    clean_tweet['location'] = tweet['location']
    clean_tweet['user_id'] = tweet['user_id']
    clean_tweet['hashtags'] = tweet['hashtags']
    return clean_tweet


def main(tweet_file):
    tweet_collection = get_tweet_collection()
    write_tweets(tweet_collection, tweet_file)


if __name__ == "__main__":
    in_file = sys.argv[1]
    main(in_file)
    # return tweet_collection
