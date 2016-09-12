import pymongo as pm
import sys
import operator
from math import sqrt
from YAML import yaml_loader
import folium


'''
This file is to extract the top k events for the three methods from mongo db.
'''


class LocationPlotter:

    def __init__(self, para):
        self.para = para
        self.n_event = para['post']['nEvent']
        self.n_tweet = para['post']['nTweetForLoc']
        self.startTS = para['post']['startTSForLoc']
        dns, port, db = para['mongo']['dns'], para['mongo']['port'], para['mongo']['db']
        self.raw_coll = self._get_collection(dns, port, db, para['mongo']['raw_tweet_col'])
        self.clean_coll = self._get_collection(dns, port, db, para['mongo']['clean_tweet_col'])
        self.entity_coll = self._get_collection(dns, port, db, para['mongo']['entity_col'])
        self.exp_coll = self._get_collection(dns, port, db, para['mongo']['exp_col'])

    def _get_collection(self, dns, port, db, col):
        conn = pm.MongoClient(dns, int(port))
        tweet_collection = conn[db][col]
        return tweet_collection

    def get_exps_from_db(self):
        bandwidth = para['hubseek']['bandwidth'][0]
        epsilon = para['hubseek']['epsilon'][0]
        eta = para['hubseek']['eta'][0]
        doc = self.exp_coll.find_one({'hubseek_stats.bandwidth': bandwidth,
                                'hubseek_stats.epsilon': epsilon,
                                'hubseek_stats.eta': eta,
                                'hubseek_stats.startTS': self.startTS,
                                'hubseek_stats.update': False})
        # print 'Document for plotting:', doc
        return doc



    def gen_events(self):
        exp = self.get_exps_from_db()
        hs = self.get_hubseek_data(exp)  # location for hubseek
        ev = self.get_eventweet_data(exp)
        # wv = self.get_wavelet_data(exp)

        hubseek_plot = [hs[0][0:50], hs[2][0:50], hs[4][0:40]]
        eventweet_plot = [ev[1], ev[6], ev[7]]
        wavelet_plot = [hs[2][-50:], ev[0],  hs[3][-50:]]
        # return hubseek_data, eventweet_data, wavelet_data
        self.write_events(hubseek_plot, para['post']['draw']['hubseek_location'])
        self.write_events(eventweet_plot, para['post']['draw']['eventweet_location'])
        self.write_events(wavelet_plot, para['post']['draw']['wavelet_location'])


    def write_events(self, event_loc_data, event_loc_file):
        m = folium.Map(location=[40.7127, -74.0059], zoom_start=12, tiles='Stamen Terrain')
        for (lat, lng) in event_loc_data[0]:
            m.polygon_marker(location=(lat, lng), fill_color='#132b5e',\
                             num_sides=4, radius=10)
        for (lat, lng) in event_loc_data[1]:
            m.polygon_marker(location=(lat, lng), fill_color='#cd0000',\
                             num_sides=5, radius=8)
        for (lat, lng) in event_loc_data[2]:
            m.polygon_marker(location=(lat, lng), fill_color='#43d9de',\
                             num_sides=3, radius=10)
        m.create_map(path = event_loc_file)

    '''
    For HubSeek.
    '''

    # get the hubseek event string
    def get_hubseek_data(self, exp):
        events = self.get_hubseek_topk_events(exp)
        ret = []
        for index, event in enumerate(events):
            e = self.get_hubseek_one_event(event)
            ret.append(e)
        return ret


    def get_hubseek_topk_events(self, exp):
        total_num = len(exp['hubseek_events'])
        num = self.n_event if total_num > self.n_event else total_num
        events = exp['hubseek_events'][:num]
        return events


    def get_hubseek_one_event(self, event):
        tweets = self.get_hubseek_tweet_list(event)
        return [(tweet['lat'], tweet['lng']) for tweet in tweets]


    def get_hubseek_tweet_list(self, event):
        sorted_tweets = self.get_sorted_tweets(event)
        tweet_list = []
        # for tweet_id, score in sorted_tweets[:self.n_tweet]:
        for tweet_id, score in sorted_tweets:
            tweet = self.clean_coll.find_one({'id':int(tweet_id)})
            tweet_list.append(tweet)
        return tweet_list


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






    '''
    For EvenTweet.
    '''
    # get the eventweet event string
    def get_eventweet_data(self, exp):
        events = self.get_eventweet_topk(exp)
        ret = []
        for index, (event, score) in enumerate(events):
            e = self.get_eventweet_one_event(exp, event)
            # e.update({'method': 'eventweet', 'event_ID': index, 'query_date': query_date, \
            #           'query_interval': exp['hubseek_stats']['endTS'] - exp['hubseek_stats']['startTS']})
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
        num = self.n_event if total_num > self.n_event else total_num
        return event_list[:num]

    def get_eventweet_one_event(self, exp, event):
        tweets = self.get_tweet_list_eventweet(exp, event)
        return [(tweet['lat'], tweet['lng']) for tweet in tweets]


    def get_tweet_list_eventweet(self, exp, event):
        tweets = self.get_range_query_sorted_tweets(exp, event)
        ret = []
        for tweet_id, score in tweets[:self.n_tweet]:
        # for tweet_id, score in tweets:
            tweet = self.clean_coll.find_one({'id':int(tweet_id)})
            ret.append(tweet)
        return ret

    def get_range_query_sorted_tweets(self, exp, event):
        start_ts = exp['hubseek_stats']['startTS']
        end_ts = exp['hubseek_stats']['endTS']
        entities = event['entityIds']
        tweets = self.clean_coll.find({'timestamp': {'$gt': start_ts, '$lt': end_ts}})
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



    '''
    For Wavelt
    '''

    # get the eventweet event string
    def get_wavelet_data(self, exp):
        events = self.get_wavelet_topk(exp)
        ret = []
        for index, (event, score) in enumerate(events):
            e = self.get_eventweet_one_event(exp, event)
            # e.update({'method': 'wavelet', 'event_ID': index, 'query_date': query_date, \
            #           'query_interval': exp['hubseek_stats']['endTS'] - exp['hubseek_stats']['startTS']})
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
        num = self.n_event if total_num > self.n_event else total_num
        return event_list[:num]


if __name__ == "__main__":
    para_file = '../../run/ny9m.yaml' if len(sys.argv) == 1 else sys.argv[1]
    para = yaml_loader().load(para_file)
    processor = LocationPlotter(para)
    processor.gen_events()
