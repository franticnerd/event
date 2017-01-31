import sys
import ast
from zutils.config.param_handler import yaml_loader
from zutils.datasets.twitter.tweet_database import TweetDatabase
from zutils.datasets.twitter.filters import EmptyMessageFilter

def load_tweet_entities(tweet_entity_file):
    fin = open(tweet_entity_file, 'r')
    line = fin.readline().strip()
    tweet_entities = ast.literal_eval(line)
    return tweet_entities


def load_clean_tweets(clean_tweet_file):
    td = TweetDatabase()
    td.load_clean_tweets_from_file(clean_tweet_file)
    return td


def link_tweet_entities(td, entities):
    for tweet in td.tweets:
        tid = str(tweet.tid)
        if tid in entities:
            tweet.message.words = entities[tid]
        else:
            tweet.message.words = []
    return td


def clean_linked_tweet_database(td, word_dict_file):
    # 1. filter the empty messages
    print 'Original size:', td.size()
    emf = EmptyMessageFilter()
    td.apply_one_filter(emf)
    print 'Size after filtering the tweets having no entities:', td.size()
    # 2. deduplication
    td.dedup()
    print 'Size after deduplication:', td.size()
    # 3. filter by word frequency
    freq_thresh = int(td.size() * 0.02) # remove the words that are too frequent
    infreq_thresh = 5
    td.trim_words_by_frequency(word_dict_file, freq_thresh, infreq_thresh)
    emf = EmptyMessageFilter()
    td.apply_one_filter(emf)
    print 'Size after filtering frequent entities:', td.size()
    # 4. rank the tweets in timestamp
    td.sort_by_time()


def write_clean_tweet_file(td, output_file):
    td.write_clean_tweets_to_file(output_file)


def run(clean_tweet_file, entity_file, word_dict_file, output_file):
    tweet_entities = load_tweet_entities(entity_file)
    td = load_clean_tweets(clean_tweet_file)
    link_tweet_entities(td, tweet_entities)
    clean_linked_tweet_database(td, word_dict_file)
    td.write_clean_tweets_to_file(output_file)


if __name__ == '__main__':
    clean_tweet_file = '/Users/chao/Dropbox/data/clean/sample/tweets.txt'
    clean_entity_file = '/Users/chao/Dropbox/data/clean/sample/entities.txt'
    word_dict_file = '/Users/chao/Dropbox/data/event/sample/word_dict.txt'
    output_file = '/Users/chao/Dropbox/data/event/sample/entity_linked_tweets.txt'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        clean_tweet_file = para['clean_tweet_file']
        clean_entity_file = para['clean_entity_file']
        word_dict_file = para['word_dict_file']
        output_file = para['entity_linked_tweet_file']
    run(clean_tweet_file, clean_entity_file, word_dict_file, output_file)

