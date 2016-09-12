import sys
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages


def load_entities(entity_file):
    entities = {}
    with open(entity_file, 'r') as fin:
        for line in fin:
            e_id, e_name, e_type = parse_entity(line)
            entities[e_id] = (e_name, e_type)
    return entities


def parse_entity(line):
    items = line.split('\t')
    entity_id = int(items[0])
    entity_name = items[1]
    entity_type = items[2]
    return entity_id, entity_name, entity_type


def load_tweets(tweet_file):
    tweets = []
    with open(tweet_file, 'r') as fin:
        for line in fin:
            lng, lat = parse_tweet(line)
            tweets.append((lng, lat))
    return tweets


def parse_tweet(line):
    items = line.split('\t')
    lng = float(items[3])
    lat = float(items[4])
    return lng, lat


def load_clusters(cluster_file):
    clusters_list = [] # a list of cluster list
    with open(cluster_file, 'r') as fin:
        for line in fin:
            clusters = parse_clusters(line)
            clusters_list.append(clusters)
    return clusters_list


def parse_clusters(line):
    clusters = []
    items = line.strip().split(',')
    for item in items:
        if item:
            coordinates = item.strip('{').strip('}').split(';')
            lng = float(coordinates[0].strip())
            lat = float(coordinates[1].strip())
            clusters.append((lng, lat))
    return clusters


def plot_cluster(tweets, clusters_list, num_init_tweet, num_tweet_frame, pdf_file):
    pp = PdfPages(pdf_file)
    for i, clusters in enumerate(clusters_list):
        start_pos = num_tweet_frame * i
        end_pos = num_init_tweet + num_tweet_frame * (i+1)
        current_tweets = tweets[start_pos:end_pos]
        fig = plt.figure()
        plt.xlim(-74.5, -73.5)
        plt.ylim(40.3, 41)
        tweet_x = [t[0] for t in current_tweets]
        tweet_y = [t[1] for t in current_tweets]
        cluster_x = [c[0] for c in clusters]
        cluster_y = [c[1] for c in clusters]
        plt.plot(tweet_x, tweet_y, 'b.', ms=2)
        plt.plot(cluster_x, cluster_y, 'ro', ms=4)
        pp.savefig(fig)
    pp.close()


if __name__ == '__main__':
    tweet_file, cluster_file, pdf_file, num_init_tweet, num_tweet_frame = sys.argv[1:6]
    tweets = load_tweets(tweet_file)
    clusters = load_clusters(cluster_file)
    plot_cluster(tweets, clusters, int(num_init_tweet), int(num_tweet_frame), pdf_file)
