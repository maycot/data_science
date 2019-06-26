# -*- coding: utf-8 -*-

"""
Train the model
"""
# Launch Tensorboard
# tensorboard --logdir=runs:experiments/base_model5/ --reload_interval 30 --port=6006

import argparse
import logging
import os

import tensorflow as tf

from model.input_fn import input_fn
from model.utils import (Params, set_logger, save_dict_to_json,
                         get_filenames_and_labels)
from model.model_fn import model_fn
from model.training import train_and_evaluate

model_name = 'base_model'

parser = argparse.ArgumentParser()
parser.add_argument('--model_dir', default='experiments/' + model_name,
                    help='Experiment directory containing params.json')
parser.add_argument('--data_dir', default='data',
                    help='Directory containing the dataset')
parser.add_argument('--restore_from', default='experiments/' + model_name + '/best_weights',
                    help='Optional, directory or file containing \
                    weights to reload before training')



def main():

    # Load the parameters from json file
    args = parser.parse_args()
    json_path = os.path.join(args.model_dir, 'params.json')
    assert os.path.isfile(json_path), 'No json configuration file found at {}'.format(json_path)
    params = Params(json_path)

    # Set the logger 
    set_logger(os.path.join(args.model_dir, 'train.log'))
    
    if not os.path.exists(args.restore_from):
        os.makedirs(args.restore_from)

    # Create the input data pipeline
    logging.info('Creating the datasets...')
    data_dir = args.data_dir
    train_data_dir = os.path.join(data_dir, 'train')
    valid_data_dir = os.path.join(data_dir, 'valid')

    # Get the filenames and labels from the train and valid sets
    train_filenames, train_labels = get_filenames_and_labels(
        train_data_dir, params)
    valid_filenames, valid_labels = get_filenames_and_labels(
        valid_data_dir, params)

    params.train_size = len(train_filenames)
    params.valid_size = len(valid_filenames)
    params.num_labels = len(set(train_labels))

    # Create the two iterators over the two datasets
    train_inputs = input_fn(True, train_filenames,
                            train_labels, params)
    valid_inputs = input_fn(False, valid_filenames,
                            valid_labels, params)

    # Define the model
    logging.info('Creating the model...')
    train_model_spec = model_fn('train', train_inputs,
                                params)
    valid_model_spec = model_fn('eval', valid_inputs,
                                params, reuse=True)
    # Train the model
    logging.info('Starting training for {} epoch(s)'.format(
        params.num_epochs))
    train_and_evaluate(train_model_spec, valid_model_spec,
                       args.model_dir, params, args.restore_from)


if __name__ == '__main__':
    main()

