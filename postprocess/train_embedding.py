import sys
from gensim.models.doc2vec import LabeledSentence
from gensim.models.doc2vec import Doc2Vec
from zutils.config.param_handler import yaml_loader

class LabeledLineSentence(object):
    def __init__(self, filename):
        self.filename = filename

    def __iter__(self):
        for uid, line in enumerate(open(self.filename)):
            yield LabeledSentence(words=line.split(), tags=['SENT_%s' % uid])

def run(message_file, embedding_file):
    sentences = LabeledLineSentence(message_file)
    model = Doc2Vec(alpha=0.025, size=100, window=15, min_alpha=0.025, workers=15)  # use fixed learning rate
    model.build_vocab(sentences)
    for epoch in range(10):
        print epoch
        model.train(sentences)
        model.alpha -= 0.002  # decrease the learning rate
        model.min_alpha = model.alpha  # fix the learning rate, no decay
    print model.most_similar('basketball')
    model.save(embedding_file)

if __name__ == '__main__':
    data_dir = '/Users/chao/Dropbox/data/event/sample/'
    if len(sys.argv) > 1:
        para_file = sys.argv[1]
        para = yaml_loader().load(para_file)
        data_dir = para['dir']
    message_file = data_dir + 'messages.txt'
    embedding_file = data_dir + 'embeddings.txt'
    run(message_file, embedding_file)
