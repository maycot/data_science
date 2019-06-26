# -*- coding: utf-8 -*-

"""
Train the model
"""
# Launch Tensorboard
# tensorboard --logdir=runs:experiments/base_model_w5_e100/train_summaries/ --reload_interval 30

import logging
import os
import pandas as pd

import tensorflow as tf

from model.utils import set_logger, save_dict_to_json
from model.training import train_and_evaluate
from model.input_fn import input_fn
from model.input_fn import load_dataset_from_text
from model.model_fn import model_fn

model_name = 'base_model_w3_e100'
data_dir = './data'
dictionary_filename = "norm_competences_structurees_dictionary.txt"
vocab_path = os.path.join(data_dir, dictionary_filename)

params = {
    #'restore_from' : None,
    'restore_from': 'experiments/' + model_name + '/last_weights',
    'model_dir': 'experiments/' + model_name,
    'num_epochs': 6,
    'train_size': 56020746,
    'eval_model_step': 1,
    'buffer_size': 100000,
    'vocab_size': 6236,
    'embedding_size': 100,
    'window_size': 3,
    'batch_size': 500,
    'num_sampled': 64,
    'save_summary_steps': 1000,
    'learning_rate': 1,
    'top_k': 8
}

train_input_filename = 'target_' + str(params['window_size']) + '.csv'
train_context_filename = 'context_' + str(params['window_size']) + '.csv'

if not os.path.exists(params['restore_from']):
    os.makedirs(params['restore_from'])

def train():
    # Set the logger
    set_logger(os.path.join(params['model_dir'], 'train.log'))
    # log params
    logging.info(params)

    # Load vacabulary
    vocab = tf.contrib.lookup.index_table_from_file(vocab_path, num_oov_buckets=1)

    # Create the input data pipeline
    logging.info('Creating the datasets...')
    train_input_words = load_dataset_from_text(data_dir, train_input_filename, vocab)
    train_context_words = load_dataset_from_text(data_dir, train_context_filename, vocab)

    # Create the iterator over the dataset
    train_inputs = input_fn('train', train_input_words, train_context_words, params)
    eval_inputs = input_fn('eval', train_input_words, train_context_words, params)
    logging.info("- done")

    # Define the model
    logging.info('Creating the model...')
    train_model_spec = model_fn('train', train_inputs, params, reuse=tf.AUTO_REUSE)
    eval_model_spec = model_fn('eval', eval_inputs, params, reuse=True)
    logging.info('- done.')

    # Train the model
    logging.info('Starting training for {} epochs'.format(params['num_epochs']))
    normalized_embedding_matrix = train_and_evaluate(
        train_model_spec, eval_model_spec, params)
    
    save_dict_to_json(params, params['model_dir']+ '/params.json')
    pd.DataFrame(normalized_embedding_matrix).to_csv(
        os.path.join(params['model_dir'],
        'normalized_embedding_matrix.tsv'), index=False, header=None, sep='\t')

    # Visualize the embeddings
    # load embedding matrix and vocab on https://projector.tensorflow.org/


if __name__ == '__main__':
    train()

