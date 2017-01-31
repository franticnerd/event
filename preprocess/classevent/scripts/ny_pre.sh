para_file='./ny_pre.yaml'
python '../gen_clean_entities.py' $para_file
python '../clean_tweets_with_entities.py' $para_file
