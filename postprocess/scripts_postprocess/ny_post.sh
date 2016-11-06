para_file='./ny_post.yaml'
# python '../train_embedding.py' $para_file
python '../gen_event_features.py' $para_file
# python '../gen_event_labels.py' $para_file
# python '../eval_event_classification.py' $para_file

