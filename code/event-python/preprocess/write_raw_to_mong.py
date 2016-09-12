import pymongo as pm
import yaml
import sys

'''
This file writes the tweets in the raw txt file into the mongodb.
Input: the raw tweet file.
'''

# This function is for parsing the YAML files that have joins.
def load_para(para_file):
    yaml.add_constructor('!join', concat)
    fin = open(para_file, 'r')
    return yaml.load(fin)

def concat(loader, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


def get_tweet_collection(para):
    dns, port, db, col = para['mongo']['dns'], para['mongo']['port'], \
        para['mongo']['db'], para['mongo']['raw_tweet_col']
    conn = pm.MongoClient(dns, int(port))
    tweet_collection = conn[db][col]
    return tweet_collection


def get_existing_tweet_ids(tweet_collection):
    existing_tweet_ids = set()
    for tweet in tweet_collection.find(projection = ['id']):
        existing_tweet_ids.add(tweet['id'])
    return existing_tweet_ids


def process_tweets(existing_tweet_ids, tweet_collection, tweet_file):
    line_cnt, batch = 0, []
    with open(tweet_file, 'r') as fin:
        for line in fin:
            try:
                tweet = parse_line(line)
                if tweet == None:
                    continue
                if check_validity(existing_tweet_ids, tweet) is True:
                    batch.append(tweet)
                    existing_tweet_ids.add(tweet['id'])
            except:
              continue
            line_cnt += 1
            if len(batch) == 10000:
                tweet_collection.insert(batch)
                batch = []
                print 'Finished processing', line_cnt, 'lines'
    tweet_collection.insert(batch)


def check_validity(existing_tweet_ids, tweet):
    if tweet['id'] in existing_tweet_ids:
        return False
    return True


def parse_line(line):
    items = line.split('\x01')
    if len(items) != 28 or items[9] != 'en':
        return None
    clean_tweet = {}
    clean_tweet['id'] = long(items[0])
    clean_tweet['text'] = items[1]
    clean_tweet['created_at'] = items[6]
    clean_tweet['location'] = items[2]
    clean_tweet['user_id'] = long(items[10])
    return clean_tweet


if __name__ == "__main__":
    para_file = sys.argv[1]
    para = load_para(para_file)
    tweet_file = para['file']['raw']['tweets']
    print 'loading tweets from:', tweet_file
    tweet_collection = get_tweet_collection(para)
    tweet_collection.remove()
    existing_tweet_ids = get_existing_tweet_ids(tweet_collection)
    print len(existing_tweet_ids), 'tweets exist in the database.'
    process_tweets(existing_tweet_ids, tweet_collection, tweet_file)
    tweet_collection.create_index("id")
    print tweet_collection.count()
