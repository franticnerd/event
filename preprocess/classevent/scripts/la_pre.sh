para_file='./la_pre.yaml'
# python '../gen_clean_entities.py' $para_file
python '../clean_tweets_with_entities.py' $para_file
python '../link_noun_and_entities.py' $para_file
