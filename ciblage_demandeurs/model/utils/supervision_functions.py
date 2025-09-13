# -*- coding: utf-8 -*-

"""
Functions used in the training to create the metrics files.
"""

import os
import logging

def create_logs_and_checkpoints_folders(model_name: str):
    """Create logs and checkpoints directories if they don't exist.
    """
    model_dir = os.path.join(model_name)
    os.makedirs(model_dir, exist_ok=True)

    logs_dir = os.path.join(model_dir, 'logs')
    os.makedirs(logs_dir, exist_ok=True)

    checkpoints_dir = os.path.join(model_dir, 'checkpoints')
    os.makedirs(checkpoints_dir, exist_ok=True)

    checkpoint_file = os.path.join(checkpoints_dir, 'checkpoint.h5')

    return model_dir, logs_dir, checkpoint_file

def set_logger(restore: bool, model_dir: str):
    """Sets the logger to log info in terminal and file in model_dir.
    ---
    Args:
        model_dir: (string) where to log
    """
    if not os.path.exists(model_dir):
        os.mkdir(model_dir)

    log_filename = os.path.join(model_dir, 'train.log')
    if not restore and os.path.isfile(log_filename):
        os.remove(log_filename)

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Logging to a file
        file_handler = logging.FileHandler(log_filename)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s:%(levelname)s:%(message)s'))
        logger.addHandler(file_handler)

        # Logging to console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(stream_handler)
