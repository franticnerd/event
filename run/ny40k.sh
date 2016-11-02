#!/bin/zsh

# parameter file
para_file='./ny40k.yaml'
java_dir='../event-java/'
python_dir='../event-python/'
jar_file=$java_dir'event-java.jar'

# --------------------------------------------------------------------------------
# Step 1: preprocessing.
# --------------------------------------------------------------------------------

function pre {

  pre_dir=$python_dir'preprocess/'

  # crawl geo-coded tweets
  # ./$python_dir'twitter-crawler/crawl.sh'

  # write raw tweets into mongodb
  # python $pre_dir'write_raw_to_mongo.py' $para_file

  # convert the tweets in the raw form (from Shaowen) to the JSON format
  # python $pre_dir/'clean_tweets_unigram.py' $para_file

  # write clean tweets and entities into mongodb
  python $pre_dir'write_clean_to_mongo.py' $para_file

  # python $pre_dir'gen_query.py' $para_file
}

# --------------------------------------------------------------------------------
# Step 2: run the algorithms.
# --------------------------------------------------------------------------------
function run {
  java -jar -Xmx4G $jar_file $para_file
}


# --------------------------------------------------------------------------------
# Step 3: post-processing
# --------------------------------------------------------------------------------

function post {
  post_dir=$python_dir'postprocess/'

  # generate the top-k events for the three methods from mongo DB.
  python $post_dir'gen_topk.py' $para_file

  # python $post_dir'plot_events.py' $input_dir'entities.txt' $output_dir'events.txt' $output_dir'events.pdf' $dns $port $db $raw_tweet_col

  # parameter for generating the time distribution histogram.
  # python $post_dir'gen_time_dist.py' $input_dir'tweets.txt' $output_dir'time_hist.txt'

  # python $post_dir'plot_clustream.py' $input_dir'tweets-reverse.txt' $output_dir'stream-clusters-loc.txt' $output_dir'stream-clusters.pdf' 2000 2000


  # plot the running time of the three methods, and the time for hubseek
  # python $post_dir'plot_time.py' $para_file
}

# pre
# run
post
