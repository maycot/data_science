# -*- coding: utf-8 -*-

"""
Train the model
"""
# Launch Tensorboard
# tensorboard --logdir=runs:experiments/base_model/ --reload_interval 30

import argparse
import logging
import os

import tensorflow as tf

from model.input_fn import input_fn
from model.utils import (Params, set_logger, save_dict_to_json,
                         get_filenames_and_labels)
from model.model_fn import model_fn
from model.evaluation import evaluate

model_name = 'base_model'

parser = argparse.ArgumentParser()
parser.add_argument('--model_dir', default='experiments/' + model_name,
                    help='Experiment directory containing params.json')
parser.add_argument('--data_dir', default='data',
                    help='Directory containing the dataset')
parser.add_argument('--restore_from', default='experiments/' + model_name + '/best_weights',
                    help='Directory or file containing the weights')

def main():

    # Load the parameters from json file
    args = parser.parse_args()
    json_path = os.path.join(args.model_dir, 'params.json')
    params = Params(json_path)

    # Set the logger 
    set_logger(os.path.join(args.model_dir, 'train.log'))

    # Create the input data pipeline
    logging.info('Creating the dataset...')
    data_dir = args.data_dir
    valid_data_dir = os.path.join(data_dir, 'valid')
    
    # Get the filenames and labels from the test set
    valid_filenames, valid_labels = get_filenames_and_labels(
        valid_data_dir, params)

    params.valid_size = len(valid_filenames)
    params.num_labels = len(set(valid_labels))

    # Create the two iterators over the two datasets
    valid_inputs = input_fn(False, valid_filenames,
                            valid_labels, params)

    # Define the model
    logging.info("Creating the model...")
    model_spec = model_fn('eval', valid_inputs, params,
                          reuse=False)

    logging.info("Starting evaluation")
    evaluate(model_spec, args.model_dir, params,
             args.restore_from)

    
if __name__ == '__main__':
    main()