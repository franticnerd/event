import pymongo as pm
import sys
import yaml
import random
import operator

'''
This file generate queries according to the configuration file, and also
writes the queries into the mongo db.
'''

# This function is for parsing the YAML files that have joins.
def load_para(para_file):
    yaml.add_constructor('!join', concat)
    fin = open(para_file, 'r')
    return yaml.load(fin)

def concat(loader, node):
    seq = loader.construct_sequence(node)
    return ''.join([str(i) for i in seq])


def get_col(dns, port, db, col):
    conn = pm.MongoClient(dns, int(port))
    collection = conn[db][col]
    return collection

# get the min and max timestamps from the clean tweet database
def get_min_max_ts(para):
    dns, port, db = para['mongo']['dns'], para['mongo']['port'], para['mongo']['db']
    tweet_col = para['mongo']['clean_tweet_col']
    tweet_collection = get_col(dns, port, db, tweet_col)
    min_ts = tweet_collection.find().sort([('timestamp', 1)]).limit(1)[0]['timestamp']
    max_ts = tweet_collection.find().sort([('timestamp', -1)]).limit(1)[0]['timestamp']
    print 'min ts:', min_ts, ' max ts:', max_ts
    return min_ts, max_ts

# get a pool of available days
def get_day_pool(para):
    min_ts, max_ts = get_min_max_ts(para)
    ref_window_size = para['query']['refWindowSize']
    start_day_index = ref_window_size / 86400 + 1
    end_day_index = max_ts / 86400
    day_pool = {}
    for i in xrange(start_day_index, end_day_index):
        day_pool[i] = [86400 * i + 3600 * 17, 86400 * i + 3600 * 24]
    return day_pool


def gen_queries(para):
    day_pool = get_day_pool(para)
    num_query = para['query']['maxNumQueryPerWindowSize']
    window_size_list = para['query']['windowSize']
    queries = []
    for l in window_size_list:
        for i in xrange(num_query):
            if len(day_pool) == 0:
                return queries
            query = gen_one_query(day_pool, l)
            if query is not None:
                queries.append(query)
    queries.sort( key = operator.itemgetter(1), reverse = False )
    return queries


def gen_one_query(day_pool, current_size):
    random_day = random.choice(day_pool.keys())
    interval_start_ts, interval_end_ts = day_pool.pop(random_day)
    if interval_end_ts - interval_start_ts < current_size:
        return None
    end = interval_end_ts - current_size
    start = interval_start_ts
    query_start = random.randint(start, end)
    query_end = query_start + current_size
    if interval_end_ts - query_end > current_size:
        day_pool[random_day] = (query_end, interval_end_ts)
    return (query_start, query_end)


# write the queries into mongo db
def write_to_mongo(queries, para):
    dns, port, db = para['mongo']['dns'], para['mongo']['port'], para['mongo']['db']
    query_col = para['mongo']['query_col']
    coll = get_col(dns, port, db, query_col)
    coll.remove()
    coll.insert({"queries": queries})

if __name__ == "__main__":
    para_file = sys.argv[1]
    para = load_para(para_file)
    if para['query']['generate'] is True:
        queries = gen_queries(para)
        print 'Number of queries: ', len(queries)
        print queries
        write_to_mongo(queries, para)
        # dns, port, db = para['mongo']['dns'], para['mongo']['port'], para['mongo']['db']
        # query_col = para['mongo']['query_col']
        # coll = get_col(dns, port, db, query_col)
        # print coll.count()
        # for q in coll.find():
        #     print len(q['queries'])
