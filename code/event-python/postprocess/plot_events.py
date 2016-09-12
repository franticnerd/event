import sys
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
import collections
import pymongo as pm


def get_tweet_collection(dns, port, db, col):
    conn = pm.MongoClient(dns, int(port))
    tweet_collection = conn[db][col]
    return tweet_collection


def get_one_tweet(col, query_id):
    return col.find_one({'id': query_id})


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


def load_clusters(cluster_file):
    # c is one cluster.
    clusters, c, points = [], [], []
    with open(cluster_file, 'r') as fin:
        for line in fin:
            if line.startswith('#'):
                if len(c) > 0:
                    c.append(points)
                    clusters.append(c)
                c, points = [], []
            elif line.startswith('Num'):
                c.append(parse_weight(line))
            elif line.startswith('Center'):
                c.append(parse_center(line))
            else:
                points.append(parse_point(line))
    if len(c) > 0:
        c.append(points)
        clusters.append(c)
    return clusters


def parse_weight(line):
    items = line.split(':')
    return float(items[1])


def parse_center(line):
    items = line.split(':')
    return items[1]

def parse_point(line):
    items = line.strip().split(',')
    print 'items', items
    lng = float(items[0].strip().strip('['))
    lat = float(items[1].strip().strip(']'))
    entities = []
    for item in items[2:]:
        entity_id = int(item.strip('[').strip(']'))
        entities.append(entity_id)
    return lng, lat, entities


def plot_cluster(col, entities, clusters, pdf_file):
    pp = PdfPages(pdf_file)
    for c in clusters:
        w, center, points = c[0], c[1], c[2]
        x = [v[0] for v in points]
        y = [v[1] for v in points]
        fig = plt.figure()
        plt.xlim(-74.3, -73.7)
        plt.ylim(40.5, 40.95)
        plt.scatter(x, y, s=100)
        s = get_string(col, center, entities, points, w)
        plt.text(-74.2, 40.75, s)
        pp.savefig(fig)
    pp.close()

def get_string(col, center, entities, points, num_tweet):
    s = '%d'% num_tweet + '\n'
    center_tweet = get_one_tweet(col, int(center))
    s += 'Center:' + center_tweet['text'] + '\n'
    common_entities = get_frequent_entities(points)
    for entity_id, count in common_entities:
        s += entities[entity_id][0] + ' ' + '%.2f'% count + '\n'
    return s

def get_frequent_entities(points):
    counter = collections.Counter()
    for p in points:
        for entity_id in p[2]:
            counter[entity_id] += 1.0
    total_sum = sum([counter[entity_id] for entity_id in counter])
    for entity_id in counter:
        counter[entity_id] /= total_sum
    return counter.most_common(8)

if __name__ == '__main__':
    entity_file, cluster_file, pdf_file, dns, port, db, col = sys.argv[1:8]
    entities = load_entities(entity_file)
    clusters = load_clusters(cluster_file)
    col = get_tweet_collection(dns, port, db, col)
    plot_cluster(col, entities, clusters, pdf_file)
