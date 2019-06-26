# -*- coding: utf-8 -*-

"""
Tensorflow utility functions for training
"""

import logging
import os

from tqdm import trange
import tensorflow as tf

from model.utils import save_dict_to_json
from model.evaluation import evaluate_sess

def train_sess(sess, model_spec, num_steps, writer, params):
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
    # Get relevant graph operations or nodes needed for training
    loss = model_spec['loss']
    train_op = model_spec['train_op']
    update_metrics = model_spec['update_metrics']
    metrics = model_spec['metrics']
    summary_op = model_spec['summary_op']
    global_step = tf.train.get_global_step()

    # Load the training dataset into the pipeline and initialize the metrics
    # local variables
    sess.run(model_spec['iterator_init_op'])
    sess.run(model_spec['metrics_init_op'])

    # Use tqdm for progress bar
    t = trange(num_steps)
    for i in t:

        # Evaluate summaries for tensorboard only once in a while
        if i % params['save_summary_steps'] == 0:
            # Perform a batch update
            _, _, loss_val, summ, global_step_val = sess.run([train_op, 
                                                              update_metrics,
                                                              loss, summary_op,
                                                              global_step])
            # Write summaries for tensorboard
            writer.add_summary(summ, global_step_val)
        else:
            _, _, loss_val = sess.run([train_op, update_metrics, loss])
        # Log the loss in the tqdm progress bar
        t.set_postfix(loss='{:05.3f}'.format(loss_val))

    metrics_values = {k: v[0] for k, v in metrics.items()}
    metrics_val = sess.run(metrics_values)
    metrics_string = ';'.join('{}: {:05.3f}'.format(k, v) 
                              for k, v in metrics_val.items())
    logging.info('- Train metrics: ' + metrics_string)

    
def train_and_evaluate(train_model_spec, eval_model_spec, params):
    """
    Train the model and evaluate every epoch.
    Args:
        train_model_spec: (dict) contains the graph operations
                          or nodes needed for training
        eval_model_spec: (dict) contains the graph operations 
                         or nodes needed for evaluation
        params: (dict) contains hyperparameters of the model.
    """
    model_dir = params['model_dir']
    eval_step = params['eval_model_step']
    restore_from = params['restore_from']
    # Initialize tf.Saver instances to save weights during training
    last_saver = tf.train.Saver() # will keep last 5 epochs
    begin_at_epoch = 0

    with tf.Session() as sess:
        # Initialize model variables
        sess.run(train_model_spec['variable_init_op'])

        # Reaload weights from directory if specified
        if os.listdir(restore_from):
            logging.info("Restoring parameters from {}".format(restore_from))
            restore_from = tf.train.latest_checkpoint(restore_from)
            begin_at_epoch = int(restore_from.split('-')[-1])
            last_saver.restore(sess, restore_from)
        
        # For tensorboard (takes care of writing summaries to files)
        train_writer = tf.summary.FileWriter(
            os.path.join(model_dir, 'train_summaries'), sess.graph)

        for epoch in range(begin_at_epoch, params['num_epochs'] - begin_at_epoch):
            # Run one epoch
            logging.info("Epoch {}/{}".format(
                epoch + 1, params['num_epochs'] - begin_at_epoch))
            # Compute number of batches in one epoch
            # (one full pass over the training set)
            num_steps = int(params['train_size'] / params['batch_size'])
            train_sess(sess, train_model_spec, num_steps, train_writer, params)
 
            # Save weights
            last_save_path = model_dir + '/last_weights/after-epoch'
            last_saver.save(sess, last_save_path, global_step=epoch + 1)
            
            # Evaluate for one step on validation set
            normalized_embedding_matrix = evaluate_sess(
            sess, eval_model_spec, eval_step, params)

        return normalized_embedding_matrix
