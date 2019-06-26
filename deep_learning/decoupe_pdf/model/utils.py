# -*- coding: utf-8 -*

"""
General utility functions
"""

import os
import json
import logging
from random import shuffle

class Params():
    """
    Class that loads hyperparameters from a json file.
    --
    Example:
        params = Params(json_path)
        print(params.learning_rate)
        params.learning_rate = 0.5 # change value
    """

    def __init__(self, json_path):
        self.update(json_path)

    def save(self, json_path):
        """Saves parameters to json file"""
        with open(json_path, 'w') as f:
            json.dump(self.__dict__, f, indent=4)

    def update(self, json_path):
        """Loads parameters from json file"""
        with open(json_path) as f:
            params = json.load(f)
            self.__dict__.update(params)

    @property
    def dict(self):
        """Gives dict-like access to Params instance
        by `params.dict['learning_rate']`"""
        return self.__dict__


def set_logger(log_path):
    """Sets the logger to log info in terminal and file `log_path`.
    In general, it is useful to have a logger so that every output
    to the terminal is saved in a permanent file.
    Here we save it to `model_dir/train.log`.
    Example:
    ```
    logging.info("Starting training...")
    ```
    Args:
        log_path: (string) where to log
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    if not logger.handlers:
        # Logging to a file
        file_handler = logging.FileHandler(log_path)
        file_handler.setFormatter(
            logging.Formatter('%(asctime)s:%(levelname)s: %(message)s'))
        logger.addHandler(file_handler)

        # Logging to console
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(logging.Formatter('%(message)s'))
        logger.addHandler(stream_handler)


def save_dict_to_json(d, json_path):
    """Saves dict of floats in json file
    Args:
        d: (dict) of float-castable values (np.float, int, float, etc.)
        json_path: (string) path to json file
    """
    with open(json_path, 'w') as f:
        # We need to convert the values to float for json 
        # (it doesn't accept np.array, np.float, )
        d = {k: str(v) for k, v in d.items()}
        json.dump(d, f, indent=4)

def get_filenames_and_labels(files_data_dir, params):
    
    # Get the filenames and labels from the train and valid sets
    filenames = [os.path.join(files_data_dir, f) 
                for f in os.listdir(files_data_dir)
                if f.endswith('.jpeg')]
    shuffle(filenames)
    
    labels = [f.split('\\')[-1].split('_')[0] for f in filenames]
    labels = [params.labelling[el] for el in labels]

    return filenames, labels

def get_type_results_dict(res_dict, bool_mask, list_of_keys):

    type_res = {}
    for k in list_of_keys:
        type_res[k] = res_dict[k][bool_mask]
    type_res['count'] = len(type_res[k])
    return type_res