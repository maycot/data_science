# -*- coding: utf-8 -*-

"""
General utility functions
"""

import logging
import json

def set_logger(log_path):
    """Sets the logger to log info in terminal and file `log_path`.
    ---
    Args:
        log_path: (string) where to log
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Logging to a file
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(logging.Formatter('%(asctime)s:%(levelname)s: %(message)s'))
        logger.addHandler(file_handler)

        # Logging to console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(stream_handler)


def save_dict_to_json(d, json_path):
    """Saves dict of floats in json file
    Args:
        d: (dict) of float values
        json_path: (string) path to json file
    """
    with open(json_path, 'w') as f:
        #d = {k: float(v) for k, v in d.items()}
        json.dump(d, f, indent=4)
