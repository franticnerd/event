import sys

from zutils.config.param_handler import yaml_loader
from zutils.datasets.twitter.tweet_database import TweetDatabase


def load_tweet_nouns(noun_db_file):
    db = load_clean_tweets(noun_db_file)
    ret = {}
    for tweet in db.tweets:
        tid = tweet.tid
        nouns = tweet.message.words
        ret[tid] = nouns
    return ret

def load_clean_tweets(clean_tweet_file):
    td = TweetDatabase()
    td.load_clean_tweets_from_file(clean_tweet_file)
    return td


def augment_with_nouns(db, nouns):
    for t in db.tweets:
        if t.tid in nouns:
            elements = nouns[t.tid]
            t.message.words.extend(elements)
        cleaned = set([w.lower() for w in t.message.words])
        t.message.words = list(cleaned)



def replace_with_nouns():
    pass



def run(noun_db_file, entity_db_file, augmented_file):
    entity_db = load_clean_tweets(entity_db_file)
    noun_dict = load_tweet_nouns(noun_db_file)
    augment_with_nouns(entity_db, noun_dict)
    entity_db.write_clean_tweets_to_file(augmented_file)


if __name__ == '__main__':
    noun_db_file = '/Users/chao/Dropbox/data/clean/sample/tweets.txt'
    entity_db_file = '/Users/chao/Dropbox/data/class_event/sample/tweets.txt'
    augmented_file = '/Users/chao/Dropbox/data/class_event/sample/tweets_noun_entity.txt'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        noun_db_file = para['clean_tweet_file']
        entity_db_file = para['entity_linked_tweet_file']
        augmented_file = para['clean_tweet_noun_entity']
    run(noun_db_file, entity_db_file, augmented_file)
