# -*- coding: utf-8 -*-

"""
Functions to map words of two lists l1 and l2 
to get a 'mapping dict' as:
   mapping_dict = {
       word_of_l2: [list of words_of_l1]
   }
Each word of l2 
(ex : words of the 'competences libres' dictionary)
is mapped to a list of words of l1 
(ex: words of the 'competences structurees' dictionary)
"""

import os
import json
import pandas as pd
import numpy as np
from time import time
from collections import defaultdict
from difflib import SequenceMatcher

def fill_dict_values_mod1(map_dict, l1, l2, ratio):
    """
    Fill dict values with sequence matching ratio between
    the words of l1 and l2
    --
    Args:
        map_dict: (dict) python dict with k (string) and v (list)
        l1, l2 : (list) list of words
        ratio : (float) ex: 0.9
    """
    for k2 in l2:
        for w1 in l1:
            if SequenceMatcher(None, w1, k2).ratio() >= ratio:
                map_dict[k2] = [w1]
                break

def fill_dict_values_mod2(map_dict, l1, l2):
    """
    Fill dict values if word of l1 is in word of l2 
    --
    Args:
        map_dict: (dict) python dict with k (string) and v (list)
        l1, l2 : (list) list of words
    """
    for k2 in l2:
        for w1 in l1:
            if w1 in k2:
                map_dict[k2] += [w1]


def return_list_of_keys_with_empty_list_value(map_dict):

    return [k for k, v in map_dict.items() if not len(v)]


def normalize_competences_libres(data_dir, comp_struct_dictionary_filename,
                                 comp_libres_dictionary_filename,
                                 map_dict_filename):
    """
    Build the mapping dict
    dict keys are w2 (words of l2) and dict values are list of w1 (words of l1)
    --
    Args:
        data_dir, filename: (string) folder and file names
    """   
    d1 = pd.read_csv(os.path.join(data_dir, comp_struct_dictionary_filename),
                     header=None, names=["w"])
    d2 = pd.read_csv(os.path.join(data_dir, comp_libres_dictionary_filename),
                     header=None, names=["w"])
    d1.dropna(inplace=True)
    d2.dropna(inplace=True)
    l1 = list(d1.w)
    l2 = list(d2.w)
    print('data loaded')

    # Create a dictionary with word of l2 in keys value
    map_dict = {k: [] for k in l2}

    # Step 1 : if sequence matching ratio between key k and words w1 in l1
    # is > 0.90, we put w1 for key k value.
    start = time()
    fill_dict_values_mod1(map_dict, l1, l2, 0.9)
    print('Step 1 done in : {} hour'.format(round((time() - start) / 3600), 2))

    # Step 2
    start = time()
    # Create a list of words of l1 with lenght >= 4
    sup4letters_l1 = [el for el in l1 if len(el) >= 4]
    # Create a liste of l2_dict keys with null value
    null_l2_keys = return_list_of_keys_with_empty_list_value(map_dict)
    print('Still {} empty values to fill'.format(len(null_l2_keys)))
    # If w1 of sup4letters_l1 is in k of null_l2_keys,
    # we put w1 in the key k list of values
    fill_dict_values_mod2(map_dict, sup4letters_l1, null_l2_keys)
    print('Step 2 done in : {} hour'.format(round((time() - start) / 3600), 2))

    # Step 3
    start = time()
    # Create a list of l2_dict keys k with value is null and lenght k >= 4
    null_sup4letters_l2_keys = [k for k, v in map_dict.items() if 
                                (not len(v) and len(k) >= 4)]
    # If sequence matching ratio between k in null_sup4letters_l2_keys and
    # w1 in l1 is >= 0.80, we put w1 in list of values for key k
    fill_dict_values_mod1(map_dict, l1, null_sup4letters_l2_keys, 0.8)
    print('Step 3 done in : {} hour'.format(round((time() - start) / 3600), 2))

    # Step 4
    start = time()
    # Create a list of words of l1 with lenght >= 4
    sup3letters_l1 = [el for el in l1 if len(el) >= 3]
    # Create a liste of l2_dict keys with null value
    null_l2_keys = return_list_of_keys_with_empty_list_value(map_dict)
    print('Still {} empty values to fill'.format(len(null_l2_keys)))
    # If w1 of sup3letters_l1 is in k of null_l2_keys, 
    # we put w1 in the key k list of values
    fill_dict_values_mod2(map_dict, sup3letters_l1, null_l2_keys)
    print('Step 4 done in : {} hour'.format(round((time() - start) / 3600), 2))
    
    # Step 5
    start = time()
    null_l2_keys = return_list_of_keys_with_empty_list_value(map_dict)
    print('Still {} empty values to fill'.format(len(null_l2_keys)))
    # If sequence matching ratio between k in null_l2_keys and w1 in l1 >= 0.8
    fill_dict_values_mod1(map_dict, l1, null_l2_keys, 0.8)
    print('Step 5 done in : {} hour'.format(round((time() - start) / 3600), 2))

    # Step 6 : 
    start = time()
    # Create a liste of l2_dict keys with null value
    null_l2_keys = return_list_of_keys_with_empty_list_value(map_dict)
    print('Still {} empty values to fill'.format(len(null_l2_keys)))
    # If w1 is in k of null_l2_keys, we put w1 in the key k list of values
    fill_dict_values_mod2(map_dict, l1, null_l2_keys)
    print('Step 6 done in : {} hour'.format(round((time() - start) / 3600), 2))
    null_l2_keys = return_list_of_keys_with_empty_list_value(map_dict)
    print('Still {} empty values to fill'.format(len(null_l2_keys)))

    with open(os.path.join(data_dir, map_dict_filename), 'wb') as f:
        f.write(json.dumps(map_dict).encode('utf-8'))


if __name__ == '__main__':
    normalize_competences_libres('../data/', 
                                 'norm_competences_structurees_dictionary.txt',
                                 'norm_competences_libres_dictionary.txt',
                                 'map_dict.json')