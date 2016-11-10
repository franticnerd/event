import sys
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator
from zutils.config.param_handler import yaml_loader
import pandas as pd
import seaborn as sns


def plot_clustream_time(input_file, output_file):
    df = pd.read_csv(input_file)
    plt.figure(figsize=(8,5))
    plt.plot(df['n_tweet'], df['time'], '-', lw=4)
    set_axis(plt, '# tweet', 'Time (sec)')
    plt.ticklabel_format(style='sci', axis='x', scilimits=(0,1))
    plt.savefig(output_file, bbox_inches='tight', pad_inches=0.08)


def plot_batch_time(input_file, output_file):
    qdf = pd.read_csv(input_file)
    fig, ax1 = plt.subplots(nrows=1, ncols=1, figsize=(8,5))
    ax1.set_ylim(-1, 25)  # set y axis range.
    ax1.set_xlim(qdf['n_tweet'].min(), qdf['n_tweet'].max())  # set y axis range.
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
    plt.savefig(output_file, bbox_inches='tight', pad_inches=0.06)


def plot_online_time(input_file, output_file):
    # time vs number of updates
    fig, ax1 = plt.subplots(nrows=1, ncols=1, figsize=(8,5))
    qdf = pd.read_csv(input_file)
    set_axis(plt, '# update', 'Time (sec)')
    ax1.plot(qdf['n_update'], qdf['delete'] + qdf['insert'], 'o-', lw=4, ms=8)
    plt.savefig(output_file, bbox_inches='tight', pad_inches=0.06)


def plot_precision(input_file, output_file):
    df = load_classification_data(input_file)
    n = df.shape[1]
    sns.set(font_scale=2.5, palette="deep", style='ticks',
            rc={'font.family': 'sans-serif', 'font.serif': 'Helvetica', \
                'legend.fontsize': 24, 'font.size': 32, 'pdf.fonttype': 42})
    sns.set_palette(sns.color_palette("Greys", n))
    # plot precision v.s. K
    fig, axes = plt.subplots(nrows=1, ncols=3, figsize=(16, 4))
    fig.tight_layout(w_pad=2.0)
    ax = df.loc[['Precision'],:].plot(kind = 'bar', ax=axes[0], legend = False)
    ax.set_ylim(0, 0.5)
    ax.get_yaxis().set_major_locator(MaxNLocator(5))
    # plt.ylim(0, 0.5)
    ax.set_xticklabels([], rotation=0)
    ax.set_ylabel('Precision')
    # plot recall v.s. Query interval
    ax = df.loc[['Recall'],:].plot(kind = 'bar', ax=axes[1], legend = False)
    plt.ylim(0, 0.7)
    ax.set_xticklabels([], rotation=0)
    ax.set_ylabel('Recall')
    ax.get_yaxis().set_major_locator(MaxNLocator(5))
    # plot f1-score v.s. Query interval
    ax = df.loc[['F1'],:].plot(kind = 'bar', ax=axes[2], legend = False)
    plt.ylim(0, 0.6)
    ax.set_xticklabels([], rotation=0)
    ax.set_ylabel('F1 Score')
    ax.get_yaxis().set_major_locator(MaxNLocator(5))
    plt.legend(bbox_to_anchor=(1.9, 1.0), ncol=1)
    plt.savefig(output_file, bbox_inches='tight', pad_inches=0.08)

def load_classification_data(input_file):
    df = pd.read_table(input_file).T
    df.columns = df.iloc[0]
    df = df[1:]
    return df

def set_axis(plt, x, y):
    plt.xlabel(x)
    plt.ylabel(y)
    plt.gca().get_xaxis().set_major_locator(MaxNLocator(5))
    plt.gca().get_yaxis().set_major_locator(MaxNLocator(5))

def set_properties():
    sns.set(font_scale=4.5, palette="deep", style='ticks',
            rc={'font.family': 'sans-serif', 'font.serif': 'Helvetica', \
                'legend.fontsize': 24, 'font.size': 36, 'pdf.fonttype': 42})


if __name__ == '__main__':
    para_file = './scripts_postprocess/ny_post.yaml' if len(sys.argv) == 1 else sys.argv[1]
    para = yaml_loader().load(para_file)
    dir = para['plot_dir']

    set_properties()
    plot_clustream_time(dir+'time_clustream.txt', dir+'time_clustream.pdf')
    plot_batch_time(dir+'time_num_tweet.txt', dir+'time_num_tweet.pdf')
    plot_online_time(dir+'time_online_update.txt', dir+'time_online_update.pdf')
    # plot_precision(dir+'classify_event_performance.txt', dir+'classify_event_performance.pdf')
