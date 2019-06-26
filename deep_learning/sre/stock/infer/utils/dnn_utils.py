# -*- coding: utf-8 -*-

import sys
import os
import logging
import numpy as np
import pandas as pd
from sklearn import metrics
import tensorflow as tf

def create_logs_and_checkpoints_folders(log_dir, save_dir, name):
    """Create logs and checkpoints directories if they don't exist.
    """
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)
        os.mkdir(os.path.join(log_dir, name))
    if not os.path.exists(os.path.join(log_dir, name)):
        os.mkdir(os.path.join(log_dir, name))
    if not os.path.exists(save_dir):
        os.mkdir(save_dir)
        os.mkdir(os.path.join(save_dir, name))
    elif not os.path.exists(os.path.join(save_dir, name)):
        os.mkdir(os.path.join(save_dir, name))

def set_logger(model_dir):
    """Sets the logger to log info in terminal and file in model_dir.
    ---
    Args:
        model_dir: (string) where to log
    """
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Logging to a file
        file_handler = logging.FileHandler(os.path.join(model_dir, 'train.log'))
        file_handler.setFormatter(logging.Formatter(
                                  '%(asctime)s:%(levelname)s:%(message)s'))
        logger.addHandler(file_handler)

        # Logging to console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(stream_handler)
    
def num_parameters(model):
    """Get total number of trainable parameters.
    """
    params = 0
    for var in tf.trainable_variables():
        params += var.get_shape().num_elements()

    return params


def auc(y_true, y_pred):
    """Get area under the curve metric.
    """
    return metrics.roc_auc_score(y_true, y_pred)

def acc(y_true, y_pred):
    """Get accuracy metric.
    """
    return metrics.accuracy_score(y_true, y_pred)