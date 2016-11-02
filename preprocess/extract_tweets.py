from zutils.datasets.twitter.tweet_database import TweetDatabase
from zutils.datasets.twitter.filters import ContainWordFilter
from word_distribution import WordEntropyProcessor
from zutils.datasets.twitter.filters import EmptyMessageFilter
from zutils.config.param_handler import yaml_loader
import sys

def load_raw_tweets(raw_tweet_file):
    td = TweetDatabase()
    td.load_raw_tweets_from_file(raw_tweet_file)
    return td

def clean_tweets(td, preserve_types, ark_run_cmd, word_dict_file, freq_thresh, infreq_thresh):
    td.clean_timestamps()
    td.tokenize_message(preserve_types, ark_run_cmd)
    td.trim_words_by_frequency(word_dict_file, freq_thresh, infreq_thresh)
    emf = EmptyMessageFilter()
    td.apply_one_filter(emf)

def find_activity_tweets(td, grid_bin_list, word_entropy_file, activity_word_fraction):
    wep = WordEntropyProcessor(td, grid_bin_list)
    wep.calc(word_entropy_file)
    activity_words = wep.select_top_words(activity_word_fraction)
    cwf = ContainWordFilter(activity_words)
    td.apply_one_filter(cwf)

if __name__ == '__main__':
    input_dir = '/Users/chao/Dropbox/data/raw/sample_tweets/'
    output_dir = '/Users/chao/Dropbox/data/event/sample/'
    # preserve_types = set(['N', '^', 'S', 'Z', 'V', 'A', '#'])
    preserve_types = set(['N', '^', 'S', 'Z'])
    ark_run_cmd='java -XX:ParallelGCThreads=2 -Xmx2G -jar /Users/chao/Dropbox/code/lib/ark-tweet-nlp-0.3.2.jar'
    freq_thresh = 250
    infreq_thresh = 5
    grid_bin_list = [50, 50, 150]
    activity_word_fraction = 0.5
    dns = 'dmserv4.cs.illinois.edu'
    port = 11111
    db_name = 'nytweets',
    col_name = 'tweets'

    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        input_dir = para['input_dir']
        output_dir = para['output_dir']
        freq_thresh = para['freq_thresh']
        infreq_thresh = para['infreq_thresh']
        preserve_types = set(para['pos_types'])
        activity_word_fraction = para['activity_word_ratio']
        ark_run_cmd = para['ark_run_cmd']

    raw_tweet_file = input_dir + 'tweets.txt'
    clean_tweet_file = output_dir + 'tweets.txt'
    word_dict_file = output_dir + 'words.txt'
    word_entropy_file = output_dir + 'entropy.txt'

    td = load_raw_tweets(raw_tweet_file)
    clean_tweets(td, preserve_types, ark_run_cmd, word_dict_file, freq_thresh, infreq_thresh)
    find_activity_tweets(td, grid_bin_list, word_entropy_file, activity_word_fraction)
    td.write_clean_tweets_to_file(clean_tweet_file)
