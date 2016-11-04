import operator
import sys
import pandas as pd

from sklearn import metrics
from sklearn.linear_model import LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import accuracy_score, f1_score
from sklearn.preprocessing import StandardScaler
from zutils.config.param_handler import yaml_loader


def load_train_test(feature_file, label_file):
    # 1. first load all the features and labels from the files
    features = pd.read_csv(feature_file, sep='\t')
    event_id_labels = []
    with open(label_file, 'r') as fin:
        for line in fin:
            items = line.strip().split('\t')
            event_id, label = int(items[0]), int(items[1])
            event_id_labels.append((event_id, label))
    event_id_labels.sort(key=operator.itemgetter(0), reverse=False)
    labels = [e[1] for e in event_id_labels]

    # 2. Use the first 80% as the training data, and the rest 20% as test data
    n_train = int(len(labels) * 0.75)
    features_train = features.head(n_train)
    labels_train = labels[:n_train]
    features_test = features.tail(len(labels) - n_train)
    labels_test = labels[n_train:]

    # 3. feature normalization
    standard_scaler = StandardScaler()
    features_train = standard_scaler.fit_transform(features_train)
    features_test = standard_scaler.transform(features_test)

    return features_train, labels_train, features_test, labels_test


def train(features, labels):
    model = LogisticRegression()
    # model = RandomForestClassifier()
    model.fit(features, labels)
    return model


def eval(model, features, labels):
    expected = labels
    predicted = model.predict(features)
    print expected
    print predicted
    print(metrics.classification_report(expected, predicted))
    # print(metrics.confusion_matrix(expected, predicted))
    # return accuracy_score(expected, predicted), f1_score(expected, predicted, average='micro'), f1_score(expected, predicted, average='macro'), f1_score(expected, predicted, average='weighted')


def run(feature_file, label_file):
    features_train, labels_train, features_test, labels_test = load_train_test(feature_file, label_file)
    model = train(features_train, labels_train)
    eval(model, features_test, labels_test)
    # print accuracy, micro_f1, macro_f1, weighted_f1


if __name__ == '__main__':
    data_dir = '/Users/chao/Dropbox/data/event/sample/'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        data_dir = para['dir']
    feature_file = data_dir + 'classify_event_features.txt'
    label_file = data_dir + 'classify_event_labels.txt'
    run(feature_file, label_file)
