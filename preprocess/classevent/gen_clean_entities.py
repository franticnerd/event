from zutils.config.param_handler import yaml_loader
import sys

def load_raw_tweet_ids(raw_tweet_file):
    tweet_ids = []
    with open(raw_tweet_file, 'r') as fin:
        for line in fin:
            items = line.strip().split('\x01')
            tweet_ids.append(items[0])
    return tweet_ids


def load_entities(raw_tweet_ids, entity_file):
    tweet_entities = {} # key: tweet_id, value: list of entities
    line_cnt = 0
    with open(entity_file, 'r') as fin:
        for line in fin:
            entities = line.strip().split()
            if len(entities) > 0:
                tweet_id = raw_tweet_ids[line_cnt]
                tweet_entities[tweet_id] = entities
            line_cnt += 1
    return tweet_entities


def write_tweet_entities(tweet_entity_file, tweet_entities):
    with open(tweet_entity_file, 'w') as fout:
        fout.write(str(tweet_entities))


def run(raw_tweet_file, entity_file, tweet_entity_file):
    raw_tweet_ids = load_raw_tweet_ids(raw_tweet_file)
    tweet_entities = load_entities(raw_tweet_ids, entity_file)
    write_tweet_entities(tweet_entity_file, tweet_entities)


if __name__ == '__main__':
    raw_tweet_file = '/Users/chao/Dropbox/data/raw/sample/tweets.txt'
    raw_entity_file = '/Users/chao/Dropbox/data/raw/sample/entities.txt'
    clean_entity_file  = '/Users/chao/Dropbox/data/clean/sample/entities.txt'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        raw_tweet_file = para['raw_tweet_file']
        raw_entity_file = para['raw_entity_file']
        clean_entity_file = para['clean_entity_file']
    run(raw_tweet_file, raw_entity_file, clean_entity_file)

