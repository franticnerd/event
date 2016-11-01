from zutils.datasets.twitter.tweet_database import TweetDatabase
from zutils.datasets.twitter.filters import ContainWordFilter
from word_distribution import WordEntropyProcessor
from zutils.datasets.twitter.filters import EmptyMessageFilter
from zutils.config.param_handler import yaml_loader
from collections import Counter
import sys
import random
from random import randint

def load_tweet_db(input_tweet_file):
    td = TweetDatabase()
    td.load_clean_tweets_from_file(input_tweet_file)
    return td

# calc word count and encode the words
def build_word_dict(td, word_dict_file):
    word_counter = td.gen_word_dict(word_dict_file)
    word_cnt_list = word_counter.rank()
    wd = {}
    word_id = 0
    for w, c in word_cnt_list:
        wd[w] = word_id
        word_id += 1
    return wd

# calc word co-occurrence info and write to file
def build_word_edges(td, wd, word_edge_file):
    ed = calc_word_cooccurrences(td, wd)
    write_word_edges(ed, word_edge_file)

def calc_word_cooccurrences(td, wd):
    edge_count = Counter()
    for tweet in td.tweets:
        word_ids = [wd[w] for w in tweet.message.words]
        pairs = set([frozenset([i, j]) for i in word_ids for j in word_ids if i != j])
        edge_count.update(pairs)
    return edge_count

def write_word_edges(ed, word_edge_file):
    with open(word_edge_file, 'w') as fout:
        for p, c in ed.items():
            word_pair = list(p)
            fout.write(','.join([str(word_pair[0]), str(word_pair[1]), str(c)]) + '\n')

def format_tweets(td, wd, clean_tweet_file):
    with open(clean_tweet_file, 'w') as fout:
        for tweet in td.tweets:
            tid = tweet.tid
            uid = tweet.uid
            ts = tweet.timestamp.timestamp
            lat = tweet.location.lat
            lng = tweet.location.lng
            words = [wd[e] for e in tweet.message.words]
            data = [tid, uid, ts, lat, lng]
            data.extend(words)
            fout.write('\t'.join([str(e) for e in data]) + '\n')


def gen_queries(td, num_query, query_lengths, query_file):
    random.seed(100)
    min_ts, max_ts = td.calc_time_range()
    # print min_ts, max_ts
    # print max_ts-min_ts
    queries = []
    for l in query_lengths:
        for i in xrange(num_query):
            start_ts = 0
            while (start_ts%86400 < 3600*8 or start_ts%86400 > 3600*23):
                start_ts = randint(min_ts, max_ts - l)
            queries.append([start_ts, start_ts + l])
            # print start_ts, (start_ts % 86400) / 3600
    with open(query_file, 'w') as fout:
        for query in queries:
            fout.write(str(query) + '\n')



def run(input_tweet_file, word_dict_file, word_edge_file, clean_tweet_file, \
        query_file, num_query, query_lengths):
    td = load_tweet_db(input_tweet_file)
    # wd = build_word_dict(td, word_dict_file)
    # build_word_edges(td, wd, word_edge_file)
    # format_tweets(td, wd, clean_tweet_file)
    gen_queries(td, num_query, query_lengths, query_file)


if __name__ == '__main__':
    input_dir = '/Users/chao/Dropbox/data/clean/sample/'
    output_dir = '/Users/chao/Dropbox/data/event/sample/'
    query_lengths = [3600]
    num_query = 100

    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        input_dir = para['clean_dir']
        output_dir = para['output_dir']
        query_lengths = para['query_lengths']
        num_query = para['num_query']

    input_tweet_file = input_dir + 'tweets.txt'
    word_dict_file = output_dir + 'words.txt'
    word_edge_file = output_dir + 'word_edges.txt'
    clean_tweet_file = output_dir + 'tweets.txt'
    query_file = output_dir + 'queries.txt'

    print input_tweet_file, word_dict_file, word_edge_file, clean_tweet_file, \
        query_file, num_query, query_lengths
    run(input_tweet_file, word_dict_file, word_edge_file, clean_tweet_file, \
        query_file, num_query, query_lengths)
