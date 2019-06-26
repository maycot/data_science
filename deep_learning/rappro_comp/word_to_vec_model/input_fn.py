# -*- coding: utf-8 -*-

"""
Create the input data pipeline using 'tf.data'
"""

import sys
import os
import pandas as pd
import tensorflow as tf

def load_dataset_from_text(data_dir, txt_filename, vocab):
    """
    Create tf.data Instance from txt file.
    --
    Args:
        txt_filename: (string) path containing one example per line
        vocab: (tf.lookuptable)
    Returns:
        dataset: (tf.Dataset) yielding list of ids of tokens for each example
    """
    # Load txt file, one example per line
    path_txt = os.path.join(data_dir, txt_filename)
    dataset = tf.data.TextLineDataset(path_txt)

    # Lookup tokens to return their ids
    dataset = dataset.map(lambda tokens: vocab.lookup(tokens))

    return dataset


def input_fn(mode, target_dataset, context_dataset, params):
    """
    Input function for word embeddings.
    --
    Args:
        mode: (string) 'train', 'eval'
        target_dataset, context_dataset: (tf.Dataset)
        params: (dict) contains hyperparameters of the model
    """
    # Load all the dataset in memory for shuffling is training
    is_training = (mode == 'train')
    buffer_size = params['buffer_size'] if is_training else 1

    dataset = tf.data.Dataset.zip((target_dataset, context_dataset))
    if is_training:
        dataset = (dataset.shuffle(buffer_size=buffer_size)
            .repeat(params['num_epochs'])
            .batch(params['batch_size'])
            .prefetch(1))
    else:
        dataset = (dataset.batch(params['batch_size'])
            .prefetch(1))
    # Create initializable iterator from this dataset so that
    # we can reset at each epoch
    iterator = dataset.make_initializable_iterator()

    # Query the output of the iterator for input to the model
    (target, context) = iterator.get_next()
    init_op = iterator.initializer

    # Build and return a dictionary containing the nodes / ops
    inputs = {
        'target': target,
        'context': context,
        'iterator_init_op': init_op
    }

    return inputs    