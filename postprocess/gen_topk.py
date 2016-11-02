import pymongo as pm
import sys
import codecs
import yaml
import operator
from math import sqrt
import time
import datetime
from dateutil.parser import parse

'''
This file is to extract the top k events for the three methods from mongo db.
'''

# This function is for parsing the YAML files that have joins.
def load_para(para_file):
    yaml.add_constructor('!join', concat)
    fin = open(para_file, 'r')
    return yaml.load(fin)


def concat(loader, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


def get_collection(dns, port, db, col):
    conn = pm.MongoClient(dns, int(port))
    tweet_collection = conn[db][col]
    return tweet_collection


def get_mongo_collections(para):
    dns, port, db = para['mongo']['dns'], para['mongo']['port'], para['mongo']['db']
    raw_coll = get_collection(dns, port, db, para['mongo']['raw_tweet_col'])
    clean_coll = get_collection(dns, port, db, para['mongo']['clean_tweet_col'])
    entity_coll = get_collection(dns, port, db, para['mongo']['entity_col'])
    exp_coll = get_collection(dns, port, db, para['mongo']['exp_col'])
    return raw_coll, clean_coll, entity_coll, exp_coll


def gen_events(para, raw_coll, clean_coll, entity_coll, exp_coll):
    n_event, n_tweet, n_entity = para['post']['nEvent'], para['post']['nTweet'], para['post']['nEntity']
    cursor = query_db(para, exp_coll)
    cnt = 1
    with codecs.open(para['post']['file']['topk'], 'w', 'utf-8') as fout:
        for e in cursor:
            hubseek_string = get_hubseek_string(e, raw_coll, entity_coll, n_event, n_tweet, n_entity)
            if hubseek_string is None:
                continue
            eventweet_string = get_eventweet_string(e, raw_coll, clean_coll, entity_coll, n_event, n_tweet, n_entity)
            wavelet_string = get_wavelet_string(e, raw_coll, clean_coll, entity_coll, n_event, n_tweet, n_entity)
            fout.write('----------- Query #' + str(cnt) + ' --------- \n')
            fout.write(get_para_string(e) + '\n')
            fout.write(hubseek_string + '\n')
            fout.write(eventweet_string + '\n')
            fout.write(wavelet_string + '\n')
            print 'Finished processing experiment:', cnt
            cnt += 1


def query_db(para, exp_coll):
    print 'Number of experiments: ', exp_coll.count()
    bandwidth = para['hubseek']['bandwidth'][0]
    epsilon = para['hubseek']['epsilon'][0]
    eta = para['hubseek']['eta'][0]
    return exp_coll.find({'hubseek_stats.bandwidth': bandwidth,
                         'hubseek_stats.epsilon': epsilon,
                         'hubseek_stats.eta': eta,
                         'hubseek_stats.update': False})


# get the parameter setting string for one exp instance
def get_para_string(exp_instance):
    ret = '* Parameters: '
    ret += 'start time: ' + to_datetime(exp_instance['hubseek_stats']['startTS'])
    ret += '; end time: ' + to_datetime(exp_instance['hubseek_stats']['endTS'])
    ret += '; interval: ' + str(exp_instance['hubseek_stats']['queryInterval'])
    # ret += str(exp_instance['hubseek_stats'])
    return ret


def to_datetime(timestamp):
    start_ts = time.mktime(parse('00:00:00 Aug 01 2014').timetuple())  # in second
    abs_ts = start_ts + timestamp
    return datetime.datetime.fromtimestamp(abs_ts).strftime('%Y-%m-%d %H:%M:%S')


'''
For HubSeek.
'''
# get the hubseek event string
def get_hubseek_string(exp_instance, raw_coll, entity_coll, K, n_tweet, n_entity):
    if len(exp_instance['hubseek_events']) < 3:
        return None
    ret = 'HubSeek Events: \n'
    total_num = len(exp_instance['hubseek_events'])
    num = K if total_num > K else total_num
    events = exp_instance['hubseek_events'][:num]
    for event in events:
        size = event['size']
        score = event['score']
        entities = event['entityTfIdf']
        # ret += '\nscore: ' + str(score) + '; ' + 'size: ' + str(size) + '\n'\
        ret += '\n' + gen_hubseek_tweet_string(raw_coll, event, n_tweet) \
            + gen_hubseek_entity_string(entity_coll, entities, n_entity) + '\n'
    return ret


def gen_hubseek_tweet_string(tweet_coll, event, n_tweet):
    sorted_tweets = get_sorted_tweets(event)
    ret = 'Tweets: \n'
    for tweet_id, score in sorted_tweets[:n_tweet]:
        tweet = tweet_coll.find_one({'id':int(tweet_id)})
        ret += tweet['text'] + '\n'
        # ret += '(' + str(tweet_id) + ' ' + str(tweet['user_id']) + ' ' + \
        #     tweet['text'] + '    ' +  str(score) + ') \n'
    return ret


# sort the entites by tf-idf
def get_sorted_tweets(event):
    tweet_ids = event['members']
    scores = event['authority']
    sort_list = []
    for i in xrange(len(tweet_ids)):
        tweet_id = tweet_ids[i]
        score = scores[i]
        sort_list.append((tweet_id, score))
    sort_list.sort( key = operator.itemgetter(1), reverse = True )
    return sort_list


def gen_hubseek_entity_string(entity_coll, entities, n_entity):
    sorted_entites = get_sorted_entities(entities)
    ret = 'Entities: \n'
    entities = []
    for entity_id, tf_idf in sorted_entites[:n_entity]:
        entity= entity_coll.find_one({'id':int(entity_id)})
        entities.append(entity['text'])
        # ret += entity['text'] + ',  '
        # ret += '(' + entity['text'] + '    ' +  str(tf_idf) + ') '
    return ret + ','.join(entities)


# sort the entites by tf-idf
def get_sorted_entities(entities):
    sort_list = []
    for entity_id in entities:
        tf_idf = entities[entity_id]
        sort_list.append((entity_id, tf_idf))
    sort_list.sort( key = operator.itemgetter(1), reverse = True )
    return sort_list


# def gen_center_tweet(raw_coll, center_tweet_id):
#     t = raw_coll.find_one({'id': int(center_tweet_id)})
#     if t is None:
#         print center_tweet_id, ' is not in the database!'
#     return t['text'] + ' ' + t['created_at']


'''
For EvenTweet.
'''
# get the eventweet event string
def get_eventweet_string(exp_instance, raw_coll, clean_coll, entity_coll, K, n_tweet, n_entity):
    ret = 'EvenTweet Events: \n'
    events = get_eventweet_topk(exp_instance, K)
    for event, score in events:
        size = event['size']
        entity_ids = event['entityIds']
        # ret += '\nscore: ' + str(score) + '; ' + 'size: ' + str(size) + '\n'\
        ret += '\n' + gen_tweet_string(exp_instance, raw_coll, clean_coll, entity_ids, n_tweet) \
            + gen_entity_string(entity_coll, entity_ids, n_entity) + '\n'
    return ret


def get_eventweet_topk(exp_instance, K):
    event_list = []
    events = exp_instance['eventweet_events']
    for event in events:
        score = event['score']
        event_list.append((event, score))
    event_list.sort( key = operator.itemgetter(1), reverse = True )
    total_num = len(exp_instance['eventweet_events'])
    num = K if total_num > K else total_num
    return event_list[:num]


def gen_tweet_string(exp_instance, raw_coll, clean_coll, entities, n_tweet):
    tweets = get_range_query_sorted_tweets(exp_instance, clean_coll, entities)
    ret = 'Tweets:\n'
    for tweet_id, score in tweets[:n_tweet]:
        tweet = raw_coll.find_one({'id':int(tweet_id)})
        ret += tweet['text'] + '\n'
        # ret += '(' + str(tweet_id) + ' ' + str(tweet['user_id']) + ' ' + \
        #     tweet['text'] + '    ' +  str(score) + ') \n'
    return ret


def get_range_query_sorted_tweets(exp_instance, clean_coll, entities):
    start_ts = exp_instance['hubseek_stats']['startTS']
    end_ts = exp_instance['hubseek_stats']['endTS']
    tweets = clean_coll.find({'timestamp': {'$gt' : start_ts, '$lt' : end_ts}})
    tweet_id_score_list = []
    for tweet in tweets:
        similarity = calc_similarity(tweet, entities)
        tweet_id_score_list.append((tweet['id'], similarity))
    tweet_id_score_list.sort( key = operator.itemgetter(1), reverse = True )
    return tweet_id_score_list


def calc_similarity(tweet, entities):
    entity_set = set(entities)
    numerator = 0
    for e in tweet['entities']:
        if e in entity_set:
            numerator += 1
    return float(numerator) / sqrt(float(len(tweet['entities'])))



# input: a dict of entity_ids. Key: index, Value: entity ID
def gen_entity_string(entity_coll, entity_ids, n_entity):
    ret = 'Entities: \n'
    for entity_id in entity_ids[:n_entity]:
        entity = entity_coll.find_one({'id':int(entity_id)})
        ret += entity['text'] + ', '
    return ret


'''
For Wavelt
'''

# get the eventweet event string
def get_wavelet_string(exp_instance, raw_coll, clean_coll, entity_coll, K, n_tweet, n_entity):
    ret = 'Wavelet Events: \n'
    events = get_wavelet_topk(exp_instance, K)
    for event, score in events:
        entity_ids = event['entityIds']
        size = len(entity_ids)
        # ret += '\nscore: ' + str(score) + '; ' + 'size: ' + str(size) + '\n' \
        ret += '\n' + gen_tweet_string(exp_instance, raw_coll, clean_coll, entity_ids, n_tweet) \
            + gen_entity_string(entity_coll, entity_ids, n_entity) + '\n'
    return ret


def get_wavelet_topk(exp_instance, K):
    event_list = []
    events = exp_instance['wavelet_events']
    for event in events:
        score = len(event['entityIds'])
        event_list.append((event, score))
    event_list.sort( key = operator.itemgetter(1), reverse = True )
    total_num = len(exp_instance['wavelet_events'])
    num = K if total_num > K else total_num
    return event_list[:num]


if __name__ == "__main__":
    para_file = sys.argv[1]
    para = load_para(para_file)
    raw_coll, clean_coll, entity_coll, exp_coll = get_mongo_collections(para)
    gen_events(para, raw_coll, clean_coll, entity_coll, exp_coll)
