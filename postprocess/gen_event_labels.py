import codecs
import json
import os.path
import sys

from zutils.config.param_handler import yaml_loader

'''
Label the events manually
'''
def label_events(description_file, label_file):
    events = load_event_descriptions(description_file)
    event_label_dict = load_event_labels(label_file)
    present_events(events, event_label_dict, label_file)

def load_event_descriptions(description_file):
    with codecs.open(description_file, 'r', 'utf-8') as fin:
        event_json = json.load(fin)
    events = {}
    for event in event_json:
        event_id = int(event['Id'])
        events[event_id] = event
    return events


def load_event_labels(label_file):
    if not os.path.exists(label_file):
        return {}
    event_label_dict = {}
    with open(label_file, 'r') as fin:
        for line in fin:
            items = line.strip().split('\t')
            event_id, label = int(items[0]), int(items[1])
            event_label_dict[event_id] = label
    return event_label_dict


def present_events(events, event_label_dict, label_file):
    unlabeled_event_ids = get_unlabeled_events(events, event_label_dict)
    while len(unlabeled_event_ids) > 0:
        event_id = unlabeled_event_ids.pop()
        present_one_event(events[event_id], event_label_dict)
        write_event_labels(event_label_dict, label_file)

def get_unlabeled_events(events, event_label_dict):
    unlabeled_events = set()
    for event_id in events:
        if event_id not in event_label_dict:
            unlabeled_events.add(event_id)
    return unlabeled_events

def present_one_event(event, event_label_dict):
    print 'Q: Do you think the following candidate is a local event?'
    print '\tRepresentative words:'
    print '\t\t\t', event['words']
    print '\tRepresentative tweets:'
    for tweet in event['tweets']:
        print '\t\t\t*', tweet
    print 'Press \'y\' for YES, and \'n\' for NO.'
    event_id = int(event['Id'])
    while True:
        line = sys.stdin.readline()
        if line.strip() == 'y':
            event_label_dict[event_id] = 1
            return
        elif line.strip() == 'n':
            event_label_dict[event_id] = 0
            return
        else:
            print 'Please use \'y\' or \'n\' to indicate your choice.'


def write_event_labels(event_label_dict, label_file):
    with open(label_file, 'w') as fout:
        for k, v in event_label_dict.items():
            fout.write(str(k) + '\t' + str(v) + '\n')


if __name__ == '__main__':
    data_dir = '/Users/chao/Dropbox/data/event/sample/'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        data_dir = para['dir']
    description_file = data_dir + 'classify_event_descriptions.txt'
    label_file = data_dir + 'classify_event_labels.txt'
    label_events(description_file, label_file)
