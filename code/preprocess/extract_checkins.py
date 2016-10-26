from zutils.datasets.foursquare.checkin_database import CheckinDatabase
from zutils.datasets.foursquare.venue_database import VenueDatabase
from zutils.datasets.twitter.filters import EmptyCategoryFilter
from zutils.datasets.twitter.filters import EmptyMessageFilter
from zutils.datasets.twitter.mongo_tweet import TweetMongo
from word_distribution import WordEntropyProcessor
from zutils.config.param_handler import yaml_loader
import sys

def load_venues(raw_venue_file):
    vd = VenueDatabase()
    vd.load_raw_from_file(raw_venue_file)
    return vd

def load_checkins(raw_checkin_file):
    td = CheckinDatabase()
    td.load_raw_tweets_from_file(raw_checkin_file)
    return td

def clean_checkins(td, vd, preserve_types, ark_run_cmd, word_dict_file, freq_thresh, infreq_thresh):
    # clean timestamp
    td.clean_timestamps()
    # clean message
    td.tokenize_message(preserve_types, ark_run_cmd)
    td.trim_words_by_frequency(word_dict_file, freq_thresh, infreq_thresh)
    emf = EmptyMessageFilter()
    td.apply_one_filter(emf)
    # clean category
    td.join_venue_database(vd)
    ecf = EmptyCategoryFilter()
    td.apply_one_filter(ecf)


def split_train_test(td, test_ratio):
    td.shuffle_tweets()
    train_td = CheckinDatabase()
    for tweet in td.tweets[:int(td.size() * 0.8)]:
        # tweet.message.raw_message = tweet.category
        train_td.add_tweet(tweet)
    test_td = CheckinDatabase()
    for tweet in td.tweets[int(td.size() * 0.8):]:
        # tweet.message.raw_message = tweet.category
        test_td.add_tweet(tweet)
    return train_td, test_td

def write_to_mongo(dns, port, db, train_td, test_td):
    tm = TweetMongo(dns, port, db, 'train')
    tm.remove_all_tweets()
    tm.write_to_mongo(train_td)
    print 'Finished writing ', tm.num_tweets(), ' tweets for training.'
    tm = TweetMongo(dns, port, db, 'test')
    tm.remove_all_tweets()
    tm.write_to_mongo(test_td)
    print 'Finished writing ', tm.num_tweets(), ' tweets for testing.'


if __name__ == '__main__':
    input_dir = '/Users/chao/Dropbox/data/raw/4sq/'
    output_dir = '/Users/chao/Dropbox/data/activity/4sq/input/'
    preserve_types = set(['N', '^', 'S', 'Z', 'V', 'A'])
    ark_run_cmd='java -XX:ParallelGCThreads=2 -Xmx2G -jar /Users/chao/Dropbox/code/lib/ark-tweet-nlp-0.3.2.jar'
    freq_thresh = 500000
    infreq_thresh = 5
    test_ratio = 0.2

    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        input_dir = para['input_dir']
        output_dir = para['output_dir']
        freq_thresh = para['freq_thresh']
        infreq_thresh = para['infreq_thresh']
        test_ratio = para['test_data_ratio']
        preserve_types = set(para['pos_types'])
        ark_run_cmd = para['ark_run_cmd']

    raw_venue_file = input_dir + 'nyc_venues.csv'
    raw_checkin_file = input_dir + 'nyc_checkins.csv'

    clean_tweet_file = output_dir + 'tweets.txt'
    clean_train_file = output_dir + 'train_checkins.txt'
    clean_test_file = output_dir + 'test_checkins.txt'
    word_dict_file = output_dir + 'words.txt'

    vd = load_venues(raw_venue_file)
    td = load_checkins(raw_checkin_file)
    clean_checkins(td, vd, preserve_types, ark_run_cmd, word_dict_file, freq_thresh, infreq_thresh)
    train_td, test_td = split_train_test(td, test_ratio)
    train_td.write_clean_tweets_to_file(clean_train_file)
    test_td.write_clean_tweets_to_file(clean_test_file)

