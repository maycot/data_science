# -*- coding: utf-8 -*-

"""
TensorFlow utility functions for evaluation
"""

import logging
import os

from tqdm import trange
import tensorflow as tf

from model.utils import save_dict_to_json

def evaluate_sess(sess, model_spec, num_steps,
                  params, writer=None):
    """
    Train the model on num_steps batches.
    ---
    Args:
        sess: (tf.Session) current session
        model_spec: (dict) contains the graph operations or 
                    nodes needed for training
        num_steps: (int) train for this number of batches
        writer: (tf.summary.FileWriter) writer for summaries
        params: (dict) hyperparameters
    """
    update_metrics = model_spec['update_metrics']
    eval_metrics = model_spec['metrics']

    # Load the evaluation dataset into the pipeline
    # and initialize init op
    sess.run(model_spec['iterator_init_op'])
    sess.run(model_spec['metrics_init_op'])

    # compute metrics over the dataset
    for _ in range(num_steps):
        sess.run(update_metrics)

    # Get the values of the metrics and embeddings
    metrics_values = {k: v[0] for k, v
                      in eval_metrics.items()}
    metrics_val = sess.run(metrics_values)
    normalized_embedding_matrix = sess.run(
        model_spec['normalized_embedding_matrix'])

    metrics_string = " ; ".join("{}: {:05.3f}".format(k, v)
        for k, v in metrics_val.items())
    logging.info("- Eval metrics: " + metrics_string)

    return normalized_embedding_matrix