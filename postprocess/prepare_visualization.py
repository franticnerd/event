import codecs
import json
import sys
from zutils.config.param_handler import yaml_loader

# extract the event locations and descriptions for visualization


def load_event_descriptions(description_file):
    with codecs.open(description_file, 'r', 'utf-8') as fin:
        event_json = json.load(fin)
    events = {}
    for event in event_json:
        event_id = int(event['Id'])
        events[event_id] = json.dumps(event, indent=2)
    return events

def load_event_labels(label_file):
    event_label_dict = {}
    with open(label_file, 'r') as fin:
        for line in fin:
            items = line.strip().split('\t')
            event_id, label = int(items[0]), int(items[1])
            event_label_dict[event_id] = label
    return event_label_dict


def load_event_locations(location_file):
    event_locations = {}
    event_id = 0
    with open(location_file, 'r') as fin:
        for line in fin:
            event_locations[event_id] = line.strip()
            event_id += 1
    return event_locations


def split_events(description_file, location_file, label_file,
                 true_event_description_file, true_event_location_file,
                 false_event_description_file, false_event_location_file):
    labels = load_event_labels(label_file)
    descriptions = load_event_descriptions(description_file)
    locations = load_event_locations(location_file)
    fout_true_des = codecs.open(true_event_description_file, 'w', 'utf-8')
    fout_true_loc = open(true_event_location_file, 'w')
    fout_false_des = codecs.open(false_event_description_file, 'w', 'utf-8')
    fout_false_loc = open(false_event_location_file, 'w')
    for event_id in xrange(len(labels)):
        print 'Event: ', event_id
        label = labels[event_id]
        event_location_str = locations[event_id]
        event_description = descriptions[event_id]
        if label == 1:
            fout_true_des.write(event_description + '\n')
            fout_true_loc.write(str(event_id) + ': ' + event_location_str + '\n')
        else:
            fout_false_des.write(event_description + '\n')
            fout_false_loc.write(str(event_id) + ': ' + event_location_str + '\n')
    fout_true_des.close()
    fout_true_loc.close()
    fout_true_des.close()
    fout_false_des.close()


if __name__ == '__main__':
    data_dir = '/Users/chao/Dropbox/data/event/sample/'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        data_dir = para['dir']
    label_file = data_dir + 'classify_event_labels.txt'
    description_file = data_dir + 'classify_event_descriptions.txt'
    location_file = data_dir + 'classify_event_locations.txt'
    true_event_description_file = data_dir + 'visualize_true_event_descriptions.txt'
    false_event_description_file = data_dir + 'visualize_false_event_descriptions.txt'
    true_event_location_file = data_dir + 'visualize_true_event_locations.txt'
    false_event_location_file = data_dir + 'visualize_false_event_locations.txt'
    split_events(description_file, location_file, label_file,
                 true_event_description_file, true_event_location_file,
                 false_event_description_file, false_event_location_file)
