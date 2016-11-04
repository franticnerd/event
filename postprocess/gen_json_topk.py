import sys
import pandas as pd
import codecs
import operator
from math import sqrt
import time
import datetime
from dateutil.parser import parse
import json
from io_utils import IO
import numpy as np

'''
This file is to extract the top k events for the three methods from mongo db.
'''

class Postprocessor:

    def __init__(self, para_file):
        self.io = IO(para_file)

    '''
    write result events into a json file
    '''
    def gen_events_to_json(self):
        exps = self.get_exps_from_db()
        self.data, cnt = [], 1
        for exp in exps:
            query_date = self.get_query_date(exp)
            hubseek_data = self.get_hubseek_data(exp, query_date)
            if hubseek_data is None:
                continue
            eventweet_data = self.get_eventweet_data(exp, query_date)
            wavelet_data = self.get_wavelet_data(exp, query_date)
            self.data.extend(hubseek_data)
            self.data.extend(eventweet_data)
            self.data.extend(wavelet_data)
            print 'Finished processing experiment:', cnt
            cnt += 1
        self.write_events()

    def get_exps_from_db(self):
        cursor = self.io.exp_coll.find({'hubseek_stats.bandwidth': self.io.default_bandwidth,
                                     'hubseek_stats.epsilon': self.io.default_epsilon,
                                     'hubseek_stats.eta': self.io.default_eta,
                                     'hubseek_stats.update': False})
        print 'Total number of experiments: ', self.io.exp_coll.count(), \
            '\nNumber of experiments for top-K generation: ', cursor.count()
        return cursor

    def get_query_date(self, exp):
        return self.to_datetime(exp['hubseek_stats']['startTS'])

    def to_datetime(self, timestamp):
        start_ts = time.mktime(parse('00:00:00 Aug 01 2014').timetuple())  # in second
        abs_ts = start_ts + timestamp
        return datetime.datetime.fromtimestamp(abs_ts).strftime('%Y-%m-%d %H:%M:%S')


    '''
    For HubSeek.
    '''
    # get the hubseek event string
    def get_hubseek_data(self, exp, query_date):
        if len(exp['hubseek_events']) < 3:
            return None
        events = self.get_hubseek_topk_events(exp)
        ret = []
        for index, event in enumerate(events):
            e = self.get_hubseek_one_event(event)
            e.update({'method': 'hubseek',  \
                      'event_ID': index, 'query_date': query_date, \
                      'startTS': exp['hubseek_stats']['startTS'], \
                      'query_interval': exp['hubseek_stats']['endTS'] - exp['hubseek_stats']['startTS']})
                    # 'event_size': len(event['members']),
            ret.append(e)
        return ret


    def get_hubseek_topk_events(self, exp):
        total_num = len(exp['hubseek_events'])
        num = self.io.json_n_event if total_num > self.io.json_n_event else total_num
        events = exp['hubseek_events'][:num]
        return events


    def get_hubseek_one_event(self, event):
        entity_text = self.get_hubseek_entity_list(event)
        ret = {'entities': ','.join(entity_text)}
        tweets = self.get_hubseek_tweet_list(event)
        for index, tweet in enumerate(tweets):
            ret['tweet_' + str(index)] = tweet
        return ret


    def get_hubseek_tweet_list(self, event):
        sorted_tweets = self.get_sorted_tweets(event)
        tweet_text_list = []
        for tweet_id, score in sorted_tweets[:self.io.json_n_tweet]:
            tweet = self.io.raw_coll.find_one({'id':int(tweet_id)})
            tweet_text_list.append(tweet['text'])
        return tweet_text_list


    # sort the entites by tf-idf
    def get_sorted_tweets(self, event):
        tweet_ids = event['members']
        scores = event['authority']
        sort_list = []
        for i in xrange(len(tweet_ids)):
            tweet_id = tweet_ids[i]
            score = scores[i]
            sort_list.append((tweet_id, score))
        sort_list.sort( key = operator.itemgetter(1), reverse = True )
        return sort_list


    def get_hubseek_entity_list(self, event):
        entities = event['entityTfIdf']
        sorted_entites = self.get_sorted_entities(entities)
        entity_text_list = []
        for entity_id, tf_idf in sorted_entites[:self.io.json_n_entity]:
            entity= self.io.entity_coll.find_one({'id':int(entity_id)})
            entity_text_list.append(entity['text'])
        return entity_text_list


    # sort the entites by tf-idf
    def get_sorted_entities(self, entities):
        sort_list = []
        for entity_id in entities:
            tf_idf = entities[entity_id]
            sort_list.append((entity_id, tf_idf))
        sort_list.sort( key = operator.itemgetter(1), reverse = True )
        return sort_list


    '''
    For EvenTweet.
    '''

    # get the eventweet event string
    def get_eventweet_data(self, exp, query_date):
        events = self.get_eventweet_topk(exp)
        ret = []
        for index, (event, score) in enumerate(events):
            e = self.get_eventweet_one_event(exp, event)
            e.update({'method': 'eventweet', 'event_ID': index, 'query_date': query_date, \
                      'query_interval': exp['hubseek_stats']['endTS'] - exp['hubseek_stats']['startTS']})
            ret.append(e)
        return ret

    def get_eventweet_topk(self, exp):
        event_list = []
        events = exp['eventweet_events']
        for event in events:
            score = event['score']
            event_list.append((event, score))
        event_list.sort( key = operator.itemgetter(1), reverse = True )
        total_num = len(exp['eventweet_events'])
        num = self.io.json_n_event if total_num > self.io.json_n_event else total_num
        return event_list[:num]

    def get_eventweet_one_event(self, exp, event):
        entity_text = self.get_entity_list_eventweet(event)
        ret = {'entities': ','.join(entity_text)}
        tweets = self.get_tweet_list_eventweet(exp, event)
        for index, tweet in enumerate(tweets):
            ret['tweet_' + str(index)] = tweet
        return ret

    def get_tweet_list_eventweet(self, exp, event):
        tweets = self.get_range_query_sorted_tweets(exp, event)
        ret = []
        for tweet_id, score in tweets[:self.io.json_n_tweet]:
            tweet = self.io.raw_coll.find_one({'id':int(tweet_id)})
            ret.append(tweet['text'])
        return ret

    def get_range_query_sorted_tweets(self, exp, event):
        start_ts = exp['hubseek_stats']['startTS']
        end_ts = exp['hubseek_stats']['endTS']
        entities = event['entityIds']
        tweets = self.io.clean_coll.find({'timestamp': {'$gt': start_ts, '$lt': end_ts}})
        tweet_id_score_list = []
        for tweet in tweets:
            similarity = self.calc_similarity(tweet, entities)
            tweet_id_score_list.append((tweet['id'], similarity))
        tweet_id_score_list.sort( key = operator.itemgetter(1), reverse = True )
        return tweet_id_score_list

    def calc_similarity(self, tweet, entities):
        entity_set = set(entities)
        numerator = 0
        for e in tweet['entities']:
            if e in entity_set:
                numerator += 1
        return float(numerator) / sqrt(float(len(tweet['entities'])))

    def get_entity_list_eventweet(self, event):
        entities = event['entityIds']
        ret = []
        for entity_id in entities[:self.io.json_n_entity]:
            entity = self.io.entity_coll.find_one({'id':int(entity_id)})
            ret.append(entity['text'])
        return ret


    '''
    For Wavelt
    '''

    # get the eventweet event string
    def get_wavelet_data(self, exp, query_date):
        events = self.get_wavelet_topk(exp)
        ret = []
        for index, (event, score) in enumerate(events):
            e = self.get_eventweet_one_event(exp, event)
            e.update({'method': 'wavelet', 'event_ID': index, 'query_date': query_date, \
                      'query_interval': exp['hubseek_stats']['endTS'] - exp['hubseek_stats']['startTS']})
            ret.append(e)
        return ret

    def get_wavelet_topk(self, exp):
        event_list = []
        events = exp['wavelet_events']
        for event in events:
            score = len(event['entityIds'])
            event_list.append((event, score))
        event_list.sort( key = operator.itemgetter(1), reverse = True )
        total_num = len(exp['wavelet_events'])
        num = self.io.json_n_event if total_num > self.io.json_n_event else total_num
        return event_list[:num]

    def write_events(self):
        with codecs.open(self.io.json_file, 'w', 'utf-8') as fout:
            for element in self.data:
                fout.write(json.dumps(element, indent = 2))
                # fout.write(json.dumps(element) + '\n')

    '''
    Extract running time from the database and write to files
    '''
    def extract_run_time_from_db(self):
        self.extract_runtime_clustream()
        self.extract_runtime_batch()
        self.extract_runtime_online()


    def extract_runtime_clustream(self):
        clustream_data = self.get_clustream_from_db()
        clustream_data.to_csv(self.io.time_clustream_file)


    def extract_runtime_batch(self):
        batch_time = self.get_batch_time_from_db()
        batch_time.to_csv(self.io.time_batch_file)


    def extract_runtime_online(self):
        online_time_ntweet, online_time_nupdates = self.get_online_time_from_db()
        online_time_ntweet.to_csv(self.io.time_online_num_tweet_file)
        online_time_nupdates.to_csv(self.io.time_online_num_update_file)


    def get_clustream_from_db(self):
        q = self.io.monary.query(self.io.db_name, self.io.exp_coll_name,
                    {'hubseek_stats.update': False},
                    ["hubseek_stats.numTweetsInClustream", "hubseek_stats.timeClustream"],
                    ["int32", "float64"])
        df = pd.DataFrame(np.matrix(q).transpose()).sort(0)
        df.columns = ['n_tweet', 'time']
        df.drop_duplicates('n_tweet', inplace = True)
        return df

    def get_batch_time_from_db(self):
        q = self.io.monary.query(self.io.db_name, self.io.exp_coll_name,
                    {'hubseek_stats.update' : False},
                    ["hubseek_stats.numTweetsHubSeek", "hubseek_stats.queryInterval",
                     "hubseek_stats.timeHubSeekBatch", "eventweet_stats.time", "wavelet_stats.time",
                     "hubseek_stats.bandwidth", "hubseek_stats.epsilon", "hubseek_stats.eta"],
                    ["int32", "int32", "float64", "float64", "float64", "float64", "float64", "float64"])
        df = pd.DataFrame(np.matrix(q).transpose()).sort(0)
        df.columns = ['n_tweet', 'interval', 'hubseek', 'eventweet', 'wavelet', 'band', 'ep', 'eta']
        # time v.s. number of tweets in the query window
        qdf = df.loc[df['band'] == self.io.default_bandwidth] \
                .loc[df['ep'] == self.io.default_epsilon] \
                .loc[df['eta'] == self.io.default_eta]
        return qdf.groupby('interval').mean()

    def get_online_time_from_db(self):
        q = self.io.monary.query(self.io.db_name, self.io.exp_coll_name,
                    {'hubseek_stats.update' : True, "hubseek_stats.bandwidth": self.io.default_bandwidth,
                     "hubseek_stats.epsilon" : self.io.default_epsilon, "hubseek_stats.eta": self.io.default_eta},
                    ["hubseek_stats.queryInterval", "hubseek_stats.numTweetsHubSeek", "hubseek_stats.numTweetsDeletion", "hubseek_stats.numTweetsInsertion",
                     "hubseek_stats.timeHubSeekBatch", "hubseek_stats.timeHubSeekDeletion", "hubseek_stats.timeHubSeekInsertion"],
                    ["int32", "int32", "int32", "int32", "float64", "float64", "float64"])
        df = pd.DataFrame(np.matrix(q).transpose()).sort(0)
        df.columns = ['interval', 'n_tweet', 'n_delete', 'n_insert', 'batch', 'delete', 'insert']
        # time vs. interval length
        qdf1 = df.groupby('interval').mean()
        # time v.s. number of updates
        median = df['n_tweet'].median()
        deviation = abs(df['n_tweet'] - median).min()
        n_tweet = median + deviation
        qdf = df.loc[df['n_tweet'] == n_tweet]
        qdf['n_update'] = qdf['n_delete'] + qdf['n_insert']
        qdf = qdf.sort('n_update')
        qdf.drop_duplicates('n_update', inplace = True)
        return qdf1, qdf


if __name__ == "__main__":
    para_file = '../../run/ny9m.yaml' if len(sys.argv) == 1 else sys.argv[1]
    processor = Postprocessor(para_file)
    # processor.gen_events_to_json()
    processor.extract_run_time_from_db()
