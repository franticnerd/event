import codecs
import json
import operator
import sys
import numpy as np
import numpy.linalg as npla
from zutils.datasets.twitter.tweet_database import TweetDatabase
from zutils.dto.text.word_dict import WordDict
from zutils.config.param_handler import yaml_loader
from gensim.models.doc2vec import Doc2Vec
from scipy.spatial.distance import cosine


def load_tweet_database(input_tweet_file):
    td = TweetDatabase()
    td.load_clean_tweets_from_file(input_tweet_file)
    return td


def load_word_dict(word_dict_file):
    wd = WordDict()
    wd.load_from_file(word_dict_file)
    return wd


def load_raw_exp_results(exp_file):
    events = []
    with open(exp_file) as fin:
        for line in fin:
            exp_json = json.loads(line)
            current_events = exp_json['hubseek_events']
            events.extend(current_events)
    print 'Number of events:', len(events)
    return events


'''
Module I. generate the features for each event;
'''
def gen_event_features(td, wd, events, embedding_file, event_feature_file):
    features = []
    overall_language_model = gen_overall_distribution(wd, events)
    embedding_model = load_embedding_model(embedding_file)
    for event_id, event in enumerate(events):
        feature = []
        feature.append(get_z_score(event))
        feature.append(get_num_tweets(event))
        feature.extend(get_day_hour(td, event))
        feature.append(get_temporal_variance(td, event))
        feature.extend(get_spatial_variance(td, event))
        feature.append(get_spatial_tf_idf_cosine(wd, event))
        feature.append(get_temporal_tf_idf_cosine(wd, event, overall_language_model))
        feature.append(get_spatial_embedding_cosine(wd, event, embedding_model))
        feature.append(get_temporal_embedding_cosine(wd, event, overall_language_model, embedding_model))
        features.append(feature)
    with open(event_feature_file, 'w') as fout:
        fout.write('\t'.join(['burstiness',
                              'n_tweet',
                              'is_weekend',
                              'hour',
                              'time_std',
                              'lat_std',
                              'lng_std',
                              'spatial_tfidf_cos',
                              'temporal_tfidf_cos',
                              'spatial_embed_cos',
                              'temporal_embed_cos']) + '\n')
        for feature in features:
            fout.write('\t'.join([str(e) for e in feature]) + '\n')

# generate a language model based on the weighted average of all the events, used in computing temporal cosine
def gen_overall_distribution(wd, events):
    word_distributions = extract_word_distributions(wd, events)
    weights = extract_sizes(events)
    average = np.zeros(wd.size())
    for weight, dist in zip(weights, word_distributions):
        average += weight * dist
    average /= npla.norm(average, 1)
    return average


def extract_word_distributions(wd, events):
    num_word = wd.size()
    language_models = []
    for event in events:
        language_models.append(to_vector(num_word, event['entityTfIdf']))
    return language_models


def extract_sizes(events):
    return [float(e['size']) for e in events]

# convert a dict to a vector, representing the probability distribution
def to_vector(dim, values):
    ret = np.zeros(dim)
    for k, v in values.items():
        ret[int(k)] = float(v)
    return ret


def get_z_score(event):
    return float(event['score'])

def get_num_tweets(event):
    return float(event['size'])

def get_day_hour(td, event):
    center_tweet_id = event['center']
    tweet = td.get_tweet(center_tweet_id)
    t = tweet.timestamp
    t.calc_day_hour()
    return 1 if t.weekday > 5 else 0, float(t.hour)

def get_spatial_variance(td, event):
    tweets = get_member_tweets(td, event)
    locations = [t.location for t in tweets]
    lats = np.array([l.lat for l in locations])
    lngs = np.array([l.lng for l in locations])
    return np.std(lats), np.std(lngs)

def get_temporal_variance(td, event):
    tweets = get_member_tweets(td, event)
    timestamps = np.array([t.timestamp.timestamp for t in tweets])
    return np.std(timestamps)

def get_member_tweets(td, event):
    tweet_ids = event['members']
    tweets = []
    for tid in tweet_ids:
        tweets.append(td.get_tweet(tid))
    return tweets

def get_spatial_tf_idf_cosine(wd, event):
    dim = wd.size()
    event_vector = to_vector(dim, event['entityTfIdf'])
    background_vector = to_vector(dim, event['backgroundTfIdf'])
    # print 'Norms: ', npla.norm(event_vector, 1), npla.norm(background_vector, 1)
    return np.dot(event_vector, background_vector)


def get_temporal_tf_idf_cosine(wd, event, overall_language_model):
    dim = wd.size()
    event_vector = to_vector(dim, event['entityTfIdf'])
    return np.dot(event_vector, overall_language_model)


def get_spatial_embedding_cosine(wd, event, embedding_model):
    dim = wd.size()
    event_distribution = to_vector(dim, event['entityTfIdf'])
    background_distribution = to_vector(dim, event['backgroundTfIdf'])
    event_words = get_top_words_from_distribution(event_distribution, wd)
    background_words = get_top_words_from_distribution(background_distribution, wd)
    event_embedding = embedding_model.infer_vector(event_words)
    background_embedding = embedding_model.infer_vector(background_words)
    print 'Representative words for event and background activities.'
    print '\t', event_words
    print '\t', background_words
    print '\t', 1.0 - cosine(event_embedding, background_embedding)
    return 1.0 - cosine(event_embedding, background_embedding)


def load_embedding_model(embedding_file):
    model = Doc2Vec.load(embedding_file)
    return model

# get the top N words from a given distribution
def get_top_words_from_distribution(distribution, wd, num = 10):
    sort_list = []
    for i, value in enumerate(distribution):
        sort_list.append((i, value))
    sort_list.sort( key = operator.itemgetter(1), reverse = True )
    word_ids = [e[0] for e in sort_list[:num]]
    return [wd.get_word(word_id) for word_id in word_ids]


def get_temporal_embedding_cosine(wd, event, overall_language_model, embedding_model):
    dim = wd.size()
    event_distribution = to_vector(dim, event['entityTfIdf'])
    event_words = get_top_words_from_distribution(event_distribution, wd)
    background_words = get_top_words_from_distribution(overall_language_model, wd)
    event_embedding = embedding_model.infer_vector(event_words)
    background_embedding = embedding_model.infer_vector(background_words)
    print 'Representative words for event and temporal activities.'
    print '\t', event_words
    print '\t', background_words
    print '\t', 1.0 - cosine(event_embedding, background_embedding)
    return 1.0 - cosine(event_embedding, background_embedding)

'''
Module II. generate the description of each event.
'''
def gen_event_descriptions(td, wd, events, event_description_file):
    descriptions = []
    for i, event in enumerate(events):
        one_event = {'Id': i}
        one_event.update(get_top_tweets(event, td))
        one_event.update(get_top_words(event, wd))
        descriptions.append(one_event)
    with codecs.open(event_description_file, 'w', 'utf-8') as fout:
        fout.write(json.dumps(descriptions, indent = 2))

# get the top N tweets for the event, return a dict
def get_top_tweets(event, td, num=10):
    # sort the tweets by authority
    tweet_ids = event['members']
    scores = event['authority']
    sort_list = []
    for i in xrange(len(tweet_ids)):
        tweet_id = tweet_ids[i]
        score = scores[i]
        sort_list.append((tweet_id, score))
    sort_list.sort( key = operator.itemgetter(1), reverse = True )
    # retrieve the tweet text from the database
    tweet_text = []
    for tweet_id, score in sort_list[:num]:
        tweet = td.get_tweet(tweet_id).message.raw_message
        tweet_text.append(tweet)
    return {'tweets': tweet_text}


# get the top N words for the event, return a dict
def get_top_words(event, wd, num=10):
    # sort the words by tf-idf
    word_tfidf = event['entityTfIdf']  # a dict, key: word id, value: tf-idf
    sort_list = []
    for word_id in word_tfidf:
        tf_idf = word_tfidf[word_id]
        sort_list.append((word_id, tf_idf))
    sort_list.sort( key = operator.itemgetter(1), reverse = True )
    # retrieve the top words
    words = []
    for word_id, tf_idf in sort_list[:num]:
        word = wd.get_word(int(word_id))
        words.append(word)
    return {'words': ','.join(words)}

def run(input_tweet_file, word_dict_file, exp_file, embedding_file, feature_file, description_file):
    td = load_tweet_database(input_tweet_file)
    td.index()
    wd = load_word_dict(word_dict_file)
    events = load_raw_exp_results(exp_file)
    gen_event_features(td, wd, events, embedding_file, feature_file)
    gen_event_descriptions(td, wd, events, description_file)


if __name__ == '__main__':
    input_tweet_file = '/Users/chao/Dropbox/data/clean/sample/tweets.txt'
    data_dir = '/Users/chao/Dropbox/data/event/sample/'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        input_tweet_file = para['clean_tweet']
        data_dir = para['dir']
    word_dict_file = data_dir + 'words.txt'
    exp_file = data_dir + 'output_events.txt'
    embedding_file = data_dir + 'embeddings.txt'
    feature_file = data_dir + 'classify_event_features.txt'
    description_file = data_dir + 'classify_event_descriptions.txt'
    run(input_tweet_file, word_dict_file, exp_file, embedding_file, feature_file, description_file)

