from zutils.datasets.twitter.tweet_database import TweetDatabase
from zutils.config.param_handler import yaml_loader
from collections import Counter
import sys
import random
import operator


def load_tweet_db(input_tweet_file):
    td = TweetDatabase()
    td.load_clean_tweets_from_file(input_tweet_file)
    return td

def dump_text(td, tweet_message_file):
    with open(tweet_message_file, 'w') as fout:
        for tweet in td.tweets:
            words = tweet.message.words
            fout.write(' '.join(words) + '\n')


# calc word co-occurrence info and write to file
def build_word_edges(td, wd, word_edge_file):
    ed = calc_word_cooccurrences(td, wd)
    write_word_edges(ed, word_edge_file)


def calc_word_cooccurrences(td, wd):
    edge_count = Counter()
    for tweet in td.tweets:
        word_ids = [wd.get_word_id(w) for w in tweet.message.words]
        pairs = set([frozenset([i, j]) for i in word_ids for j in word_ids if i != j])
        edge_count.update(pairs)
    return edge_count


def write_word_edges(ed, word_edge_file):
    with open(word_edge_file, 'w') as fout:
        for p, c in ed.items():
            word_pair = list(p)
            fout.write('\t'.join([str(word_pair[0]), str(word_pair[1]), str(c)]) + '\n')


def format_tweets(td, wd, clean_tweet_file):
    with open(clean_tweet_file, 'w') as fout:
        for tweet in td.tweets:
            tid = tweet.tid
            uid = tweet.uid
            ts = tweet.timestamp.timestamp
            lat = tweet.location.lat
            lng = tweet.location.lng
            word_ids = [wd.get_word_id(w) for w in tweet.message.words]
            data = [tid, uid, ts, lat, lng]
            data.extend(word_ids)
            fout.write('\t'.join([str(e) for e in data]) + '\n')


def gen_queries(td, num_query, query_lengths, ref_window_size, query_file):
    random.seed(100)
    min_ts, max_ts = td.calc_time_range()
    day_pool = get_day_pool(min_ts, max_ts, ref_window_size)
    # print day_pool
    queries = []
    for l in query_lengths:
        for i in xrange(num_query):
            if len(day_pool) == 0:
                return queries
            query = gen_one_query(day_pool, l)
            if query is not None:
                queries.append(query)
    queries.sort(key = operator.itemgetter(1), reverse = False)
    with open(query_file, 'w') as fout:
        for query in queries:
            fout.write(str(query[0]) + '\t' + str(query[1]) + '\n')


# get a pool of available days
def get_day_pool(min_ts, max_ts, ref_window_size):
    start_day_index = (min_ts + ref_window_size) / 86400
    end_day_index = max_ts / 86400
    day_pool = {}
    for i in xrange(start_day_index, end_day_index):
        day_pool[i] = [86400 * i + 3600 * 10, 86400 * i + 3600 * 24]
    return day_pool


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


def run(input_tweet_file, tweet_message_file, word_dict_file, word_edge_file, clean_tweet_file, \
        query_file, num_query, query_lengths, ref_window_size):
    td = load_tweet_db(input_tweet_file)
    wd = td.gen_word_dict(word_dict_file)
    dump_text(td, tweet_message_file)
    # build_word_edges(td, wd, word_edge_file)
    # format_tweets(td, wd, clean_tweet_file)
    # gen_queries(td, num_query, query_lengths, ref_window_size, query_file)


if __name__ == '__main__':
    input_dir = '/Users/chao/Dropbox/data/clean/sample/'
    output_dir = '/Users/chao/Dropbox/data/event/sample/'
    num_query = 2
    query_lengths = [3600]
    ref_window_size = 86400

    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        input_dir = para['clean_dir']
        output_dir = para['output_dir']
        query_lengths = para['query_lengths']
        num_query = para['num_query']
        ref_window_size = para['refWindowSize']

    input_tweet_file = input_dir + 'tweets.txt'
    tweet_message_file = output_dir + 'messages.txt'
    word_dict_file = output_dir + 'words.txt'
    word_edge_file = output_dir + 'word_edges.txt'
    clean_tweet_file = output_dir + 'tweets.txt'
    query_file = output_dir + 'queries.txt'

    run(input_tweet_file, tweet_message_file, word_dict_file, word_edge_file, clean_tweet_file, \
        query_file, num_query, query_lengths, ref_window_size)
