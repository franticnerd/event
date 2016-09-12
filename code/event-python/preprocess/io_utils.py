import pymongo as pm
from zutils import parameter
from monary import Monary

class IO:

    def __init__(self, para_file):
        self.init_para(para_file)
        self.init_files()
        self.init_db()


    def init_db(self):
        dns = self.para['mongo']['dns']
        port = int(self.para['mongo']['port'])
        self.db_name = self.para['mongo']['db']
        conn = pm.MongoClient(dns, port)
        self.db = conn[self.db_name]
        self.raw_col = self.db[self.para['mongo']['raw_tweet_col']]
        self.clean_coll = self.db[self.para['mongo']['clean_tweet_col']]
        self.entity_coll = self.db[self.para['mongo']['entity_col']]
        self.exp_coll_name = self.para['mongo']['exp_col']
        self.exp_coll = self.db[self.exp_coll_name]
        self.monary = Monary(dns, port)



    def init_files(self):
        # files for drawing
        self.json_file = self.para['post']['file']['json']
        self.precision_k_file = self.para['post']['file']['precision_k']
        self.precision_time_file = self.para['post']['file']['precision_time']

        self.time_batch_file = self.para['post']['file']['time_num_tweet']
        self.time_clustream_file = self.para['post']['file']['time_clustream']
        self.time_online_num_tweet_file = self.para['post']['file']['time_online_num_tweet']
        self.time_online_num_update_file = self.para['post']['file']['time_online_num_update']
        # output pdf files for drawing
        self.precision_pdf = self.para['post']['draw']['precision_k']
        self.time_clustream_pdf = self.para['post']['draw']['time_clustream']
        self.time_batch_pdf = self.para['post']['draw']['time_num_tweet']
        self.time_online_pdf = self.para['post']['draw']['time_online_num_tweet']

    def init_para(self, para_file):
        self.para = parameter.yaml_loader().load(para_file)
        # parameters for hubseek
        self.bandwidth_list = self.para['hubseek']['bandwidth']
        self.epsilon_list = self.para['hubseek']['epsilon']
        self.eta_list = self.para['hubseek']['eta']
        self.default_bandwidth = self.bandwidth_list[0]
        self.default_epsilon = self.epsilon_list[0]
        self.default_eta = self.eta_list[0]
        # parameters for writing Json file
        self.n_event = self.para['post']['nEvent']
        self.n_tweet = self.para['post']['nTweet']
        self.n_entity = self.para['post']['nEntity']

    def remove_exp_col(self):
        self.exp_coll.remove()


