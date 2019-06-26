# -*- coding: utf-8 -*-

"""
Tensorflow utility functions for evaluation
"""

import logging
import os
import numpy as np

from tqdm import trange
from collections import defaultdict
import tensorflow as tf 

from model.utils import (save_dict_to_json,
                         get_type_results_dict)
                         
def evaluate_sess(sess, model_spec, num_steps, 
                  writer=None, params=None):
    """
    Train the model on 'num_steps' batches.
    --
    Args:
        sess: (tf.Session) current session
        model_spec: (dict) contains the graph operations 
                    or nodes for training
        num_steps: (int) train for this number of batches
        writer: (tf.summary.FileWriter) writer for summaries.
        params: (Params) hyperparameters
    """
    update_metrics = model_spec['update_metrics']
    eval_metrics = model_spec['metrics']
    global_step = tf.train.get_global_step()

    # Load the evaluation dataset into the pipeline
    # and initialize the metrics init op
    sess.run(model_spec['iterator_init_op'])
    sess.run(model_spec['metrics_init_op'])
    
    # compute metrics over the dataset
    #for _ in range(num_steps):
    for _ in range(num_steps):
        sess.run(update_metrics)

    # Get the values of the metrics
    metrics_values = {k: v[0] for k, v in eval_metrics.items()}
    metrics_val = sess.run(metrics_values)
    metrics_string = ' ; '.join('{}: {:05.3f}'.format(
                           k, v) for k, v in metrics_val.items())
    logging.info('- Eval metrics: ' + metrics_string)

    # Add summaries manually to writer at global_step_val
    if writer is not None:
        global_step_val = sess.run(global_step)
        for tag, val in metrics_val.items():
            summ = tf.Summary(value=[tf.Summary.Value(
                tag=tag, simple_value=val)])
            writer.add_summary(summ, global_step_val)
    
    return metrics_val

def results_dict(sess, model_spec, num_steps):
    """
    Returns dicts with labels, filenames, predictions
    for wrong and right results.
    --
    Args:
        sess: (tf.Session) current session
        model_spec: (dict) contains the graph operations 
                    or nodes for training
        num_steps: (int) train for this number of batches
    """
    res_dict = defaultdict(list)

    for el in ['labels', 'predictions', 'filenames']:
        sess.run(model_spec['iterator_init_op'])
        for _ in range(num_steps):
            res_dict[el].append(sess.run(model_spec[el]))

    for k in ['labels', 'predictions', 'filenames']:
        res_dict[k] = np.concatenate(res_dict[k]).ravel()

    mask = res_dict['labels'] == res_dict['predictions']
    list_of_keys = ['labels', 'predictions', 'filenames']
    wrong_res = get_type_results_dict(res_dict, ~mask, list_of_keys)
    right_res = get_type_results_dict(res_dict, mask, list_of_keys)

    return wrong_res, right_res

def evaluate(model_spec, model_dir, params, restore_from):
    """
    Evaluate the model
    --
    Args:
        model_spec: (dict) contains the graph operations
                    or nodes needed for evaluation
        model_dir: (string) directory containing config,
                   weights and log
        params: (Params) contains hyperparameters of the model
        restore_from: (string) directory constaining weights
                      to restore the graph
    """
    # Initialize tf.Saver
    saver = tf.train.Saver()

    with tf.Session() as sess:
        sess.run(model_spec['variable_init_op'])

        # Reload weights from the weights subdirectory
        if os.listdir(restore_from):
            save_path = tf.train.latest_checkpoint(restore_from)
        saver.restore(sess, save_path)
    
        # Evaluate
        num_steps = int(params.valid_size / params.batch_size)
        metrics = evaluate_sess(sess, model_spec, num_steps)
        wrong_res, right_res = results_dict(sess, model_spec,
                                           num_steps)
                                        
        wrong_res['filenames'] = [el.decode().split('\\')[-1] 
                                  for el in wrong_res['filenames']]
        right_res['filenames'] = [el.decode().split('\\')[-1] 
                                  for el in right_res['filenames']]
        save_dict_to_json(metrics, model_dir + '/test_metrics.json')
        save_dict_to_json(wrong_res, model_dir + '/wrong_res.json')
        save_dict_to_json(right_res, model_dir + '/right_res.json')