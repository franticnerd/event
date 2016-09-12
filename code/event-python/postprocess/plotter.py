import sys
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
import pandas as pd
import seaborn as sns
from io_utils import IO

class Plotter:

    def __init__(self, para_file):
        self.io = IO(para_file)

    def set_properties(self):
        sns.set(font_scale=2.75, \
                palette="deep",\
                style='ticks',\
                rc = {'font.family': 'sans-serif', 'font.serif':'Helvetica', \
                      'legend.fontsize':24,
                      'font.size':36, 'pdf.fonttype': 42})


    def plot_precision(self):
        # plot precision v.s. K
        df = pd.read_table(self.io.precision_k_file)
        fig, axes = plt.subplots(nrows=1, ncols=2, figsize=(16,4))
        ax = df.loc[:,['GeoBurst','EvenTweet', 'Wavelet']].plot(kind = 'bar', ax=axes[0], legend = False)
        ax.get_yaxis().set_major_locator(MaxNLocator(5))
        ax.set_xticklabels(df['Rank'], rotation=0)
        ax.set_xlabel('K')
        ax.set_ylabel('Precision')
        # plot precision v.s. Query interval
        df = pd.read_table(self.io.precision_time_file)
        ax = df.loc[:,['GeoBurst','EvenTweet', 'Wavelet']].plot(kind = 'bar', ax=axes[1])
        ax.set_xticklabels(df['time'], rotation=0)
        ax.set_xlabel('Query interval (hour)')
        ax.set_ylabel('')
        ax.get_yaxis().set_major_locator(MaxNLocator(5))
        ax.legend(bbox_to_anchor=(0.6, 1.3), ncol=3)
        plt.savefig(self.io.precision_pdf, bbox_inches='tight', pad_inches=0.08)

    def plot_clustream_time(self):
        df = pd.read_csv(self.io.time_clustream_file)
        plt.figure(figsize=(8,4))
        # plt.xlim(df['n_tweet'].min(), df['n_tweet'].max())  # set y axis range.
        # plt.ylim(0, df['time'].max()*1.25)  # set y axis range.
        plt.plot(df['n_tweet'], df['time'], '-', lw=4)
        self.set_axis(plt, '# processed tweet', 'Time (sec)')
        plt.ticklabel_format(style='sci', axis='x', scilimits=(0,1))
        plt.savefig(self.io.time_clustream_pdf, bbox_inches='tight', pad_inches=0.08)


    def plot_batch_time(self):
        qdf = pd.read_csv(self.io.time_batch_file)
        fig, ax1 = plt.subplots(nrows=1, ncols=1, figsize=(8,4))
        # max_time = qdf['hubseek'].append(qdf['eventweet']).max()
        # max_time = max(max_time, qdf['wavelet'].mean())
        ax1.set_ylim(-1, 25)  # set y axis range.
        ax1.set_xlim(qdf['n_tweet'].min(), qdf['n_tweet'].max())  # set y axis range.
        # set_axis(plt, '# tweet', 'time (sec)')
        ax1.get_xaxis().set_major_locator(MaxNLocator(5))
        ax1.get_yaxis().set_major_locator(MaxNLocator(5))
        ax1.set_xlabel('# query tweet')
        ax1.set_ylabel('Time (sec)')
        ax1.ticklabel_format(style='sci', axis='x', scilimits=(0,0))
        p0, = ax1.plot(qdf['n_tweet'], qdf['online'], 'o-', lw=4, ms=14)
        p1, = ax1.plot(qdf['n_tweet'], qdf['hubseek'], 's--', lw=4, ms=14)
        p2, = ax1.plot(qdf['n_tweet'], qdf['eventweet'], 'v-.', lw=4, ms=14)
        p3, = ax1.plot(qdf['n_tweet'], [qdf['wavelet'].mean()] * len(qdf['n_tweet']), 'd:', lw=4, ms=14)
        # fig.legend([p1, p2, p3], ['GeoBurst', 'EvenTweet', 'Wavelet'], bbox_to_anchor=[0.95, 1.15], ncol=3, numpoints=1)
        plt.legend([p0, p1, p2, p3], ['GeoBurst-Online', 'GeoBurst-Batch', 'EvenTweet', 'Wavelet'], loc=[-0.1,1.05], ncol = 2, numpoints=1)
        plt.savefig(self.io.time_batch_pdf, bbox_inches='tight', pad_inches=0.06)


    def plot_online_time(self):
        # time vs number of tweets
        # qdf = pd.read_csv(self.io.time_online_num_tweet_file)
        # fig, (ax1, ax2) = plt.subplots(nrows=1, ncols=2, figsize=(16,5))
        # ax1.get_xaxis().set_major_locator(MaxNLocator(5))
        # ax1.get_yaxis().set_major_locator(MaxNLocator(5))
        # ax1.set_xlabel('# query tweet')
        # ax1.set_ylabel('Time (sec)')
        # ax1.ticklabel_format(style='sci', axis='x', scilimits=(0,0))
        # ax1.set_xlim(qdf['n_tweet'].min(), qdf['n_tweet'].max())  # set y axis range.
        # plt.ylim(0, qdf['batch'].max()*1.25)  # set y axis range.
        # p1, = ax1.plot(qdf['n_tweet'], qdf['batch'], 'o--', lw=4, ms=14)
        # p2, = ax1.plot(qdf['n_tweet'], qdf['delete'] + qdf['insert'], 's-', lw=4, ms=14)
        # ax1.legend([p1, p2], ['Batch', 'Online'], loc='best', numpoints=1)
        # fig.legend([p1, p2], ['Batch', 'Online'], loc='best', numpoints=1)
        # output_file = para['post']['draw']['time_online_num_tweet']
        # plt.savefig(output_file, bbox_inches='tight', pad_inches=0.06)
        # time vs number of updates
        fig, ax1 = plt.subplots(nrows=1, ncols=1, figsize=(8,4))
        qdf = pd.read_csv(self.io.time_online_num_update_file)
        self.set_axis(plt, '# update', 'Time (sec)')
        # ax1.set_ylim(0, qdf['batch'].mean()*1.25)  # set y axis range.
        # ax1.set_xlim(qdf['n_update'].min(), qdf['n_update'].max())  # set y axis range.
        ax1.plot(qdf['n_update'], qdf['delete'] + qdf['insert'], 'o-', lw=4, ms=8)
        # plt.legend([p1, p2], ['Batch', 'Online'], loc='best', numpoints=1)
        # ax1.legend([p1, p2], ['Batch', 'Online'], loc='center left', numpoints=1)
        plt.savefig(self.io.time_online_pdf, bbox_inches='tight', pad_inches=0.06)


    def set_axis(self, plt, x, y):
        plt.xlabel(x)
        plt.ylabel(y)
        plt.gca().get_xaxis().set_major_locator(MaxNLocator(5))
        plt.gca().get_yaxis().set_major_locator(MaxNLocator(5))


if __name__ == '__main__':
    para_file = '../../run/ny9m.yaml' if len(sys.argv) == 1 else sys.argv[1]
    p = Plotter(para_file)
    p.set_properties()
    p.plot_precision()
    p.plot_clustream_time()
    p.plot_batch_time()
    p.plot_online_time()
