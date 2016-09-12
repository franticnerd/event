import sys
import time
import json
from dateutil.parser import parse
from datetime import datetime
from collections import Counter

'''
this file is to generate the time distribution for the tweets.
Input: the tweet file, each line is a tweet in JSON
Output: the time distribution hist information.
'''

start_time_string = 'Jul 31 00:00:00 CDT 2014'
time_format = '%b %d %H:%M:%S %Z %Y'

def count(tweet_file, counter_file, width):
    counter = Counter()
    line_cnt = 0
    with open(tweet_file, 'r') as fin:
        for line in fin:
            ts = parse_tweet(line)
            index = int(ts / width)
            counter[index] += 1
            line_cnt += 1
            if line_cnt % 1000 == 0:
                print 'line ', line_cnt
    with open(counter_file, 'w') as fout:
        fout.write(str(counter))


def parse_tweet(line):
    start_timestamp = time.mktime(datetime.timetuple(parse(start_time_string)))
    tweet = json.loads(line)
    time_string = tweet['created_at']
    current_timestamp = time.mktime(datetime.timetuple(parse(time_string)))
    timestamp = current_timestamp - start_timestamp
    return timestamp


if __name__ == '__main__':
    tweet_file, counter_file = sys.argv[1:3]
    width = 1800
    count(tweet_file, counter_file, int(width))
