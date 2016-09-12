import sys
import yaml
import Word
import timestamp
import pymongo as pm
from collections import Counter

'''
This file is to convert the raw tweets to the clean tweets in JSON format.
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


# generate the keywords as well as the stopwords
def process_keywords(coll, tweet_file, keyword_file, stopword_file):
    c = gen_counter(coll)
    stopwords = gen_stopwords(c, stopword_file)
    write_stopwords(stopwords, stopword_file)
    keywords = gen_keywords(c, stopwords)
    word_dict = write_keywords(keywords, keyword_file)
    return word_dict

# generate word counter from the raw data
def gen_counter(coll):
    c = Counter()
    processor = Word.Word(True, 3) # minimum length is 3
    # cnt = 0
    for tweet in coll.find().sort('created_at'):
        s = tweet['text']
        words = processor.parse_words(s)
        c.update(words)
        # cnt += 1
        # if cnt == 100:
        #     return c
    return c

# generate stopwords
def gen_stopwords(c, stopword_file):
    stopwords = set()
    for word, cnt in c.most_common(10):
        stopwords.add(word)
    for word in c:
        if c[word] <= 3:
            stopwords.add(word)
    return stopwords

def write_stopwords(stopwords, stopword_file):
    sw_list = list(stopwords)
    sw_list.sort()
    with open(stopword_file, 'w') as fout:
        for word in sw_list:
            fout.write(word + '\n')

def gen_keywords(c, stopwords):
    word_cnt_list = c.most_common()
    keywords = []
    for word, cnt in word_cnt_list:
        if word not in stopwords:
            keywords.append((word, cnt))
    return keywords

def write_keywords(keywords, keyword_file):
    word_dict, word_id = {}, 0
    with open(keyword_file, 'w') as fout:
        for word, cnt in keywords:
            fout.write('\t'.join([str(word_id), word, str(cnt)]) + '\n')
            word_dict[word] = word_id
            word_id += 1
    return word_dict


# geenrate clean tweets according to the stopwords
def process_tweets(coll, word_dict, tweet_file):
    # cnt = 0
    cooccurrence = Counter()  # this dict keeps the co-occurrence info for keywords
    min_ts = get_min_ts(coll)
    with open(tweet_file, 'w') as fout:
        for tweet in coll.find().sort('created_at'):
            fout.write(tweet_to_string(tweet, min_ts, word_dict, cooccurrence) + '\n')
            # cnt += 1
            # if cnt == 10:
            #     return cooccurrence
    return cooccurrence

def get_min_ts(coll):
    return coll.find().sort([('created_at', 1)]).limit(1)[0]['created_at']

def tweet_to_string(tweet, min_ts, word_dict, cooccurrence):
    tid = str(tweet['id'])
    uid = str(tweet['user_id'])
    ts = str(gen_timestamp(tweet, min_ts))
    ls = gen_loc_string(tweet)
    words = gen_word_string(tweet, word_dict, cooccurrence)
    return '\t'.join([tid, uid, ts, ls, words])

def gen_timestamp(tweet, start):
    t = timestamp.Timestamp(start)
    return t.get_timestamp(tweet['created_at'], 'sec')

def gen_word_string(tweet, word_dict, cooccurrence):
    processor = Word.Word(True, 3) # minimum length is 3
    s = tweet['text']
    words = processor.parse_words(s)
    word_ids = []
    for w in words:
        if w in word_dict:
            word_ids.append(word_dict[w])
    update_cooccurrence(cooccurrence, word_ids)
    return '\t'.join([str(wid) for wid in word_ids])


def update_cooccurrence(cooccurrence, word_ids):
    for i in xrange(len(word_ids) - 1):
        for j in xrange(1, len(word_ids)):
            s = frozenset((word_ids[i], word_ids[j]))
            cooccurrence[s] += 1

def gen_loc_string(tweet):
    return '\t'.join([str(coord) for coord in tweet['location']])




# write entity edges to file
def write_edges(coll, cooccurrence, edge_file):
    with open(edge_file, 'w') as fout:
        for s in cooccurrence:
            cnt = cooccurrence[s]
            if cnt >= 3 and len(s) == 2:
                word_s = '\t'.join([str(e) for e in s])
                fout.write(word_s + '\t' + str(cnt) + '\n')


if __name__ == "__main__":
    para_file = sys.argv[1]
    para = load_para(para_file)
    coll = get_tweet_collection(para)
    clean_tweet_file = para['file']['input']['uni_tweets']
    clean_entity_file = para['file']['input']['uni_entities']
    clean_edge_file = para['file']['input']['uni_edges']
    stopword_file = para['file']['input']['stopwords']
    # print input_file, output_file
    word_dict = process_keywords(coll, clean_tweet_file, clean_entity_file, stopword_file)
    keyword_cooccur = process_tweets(coll, word_dict, clean_tweet_file)
    write_edges(coll, keyword_cooccur, clean_edge_file)
