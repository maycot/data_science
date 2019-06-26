# -*- coding: utf-8 -*-

"""
General utility functions to manipulate dataset
"""

import sys
import os
import pandas as pd
from time import time

def load_csv_to_df(path, filename):
    """Load csv file and return pandas dataframe.
    """
    start = time()
    df = pd.read_csv(os.path.join(path, filename), sep=';', dtype='unicode')
    df.columns = [el.split(".")[1] for el in df.columns]
    print("Load csv to dataframe in {} min.".format(round(
                                                    (time() - start) / 60)))
    return df


def dump_df_to_csv(df, path, filename):
    """Dump pandas df to csv file.
    """
    df.to_csv(os.path.join(path, filename), index=False)


def split_dataset(df, feature_to_keep_ratio, number_of_datasets_to_generate,
                  data_dir, data_filename):
    """Split df in smaller and balanced datasets and write them to disk.

    Parameters:
    ---
    feature_to_keep_ratio (string): must have only two modalities
    number_of_datasets_to_generate (int): number of balanced datasets to make
    """
    start = time()
    mod_list = list(set(df[feature_to_keep_ratio]))
    first_mod_df = df[df[feature_to_keep_ratio] == mod_list[0]]
    second_mod_df = df[df[feature_to_keep_ratio] == mod_list[1]]

    for i in range(number_of_datasets_to_generate):
        first_mod_df_sample = first_mod_df[i::number_of_datasets_to_generate]
        second_mod_df_sample = second_mod_df[i::number_of_datasets_to_generate]
        sample_df = first_mod_df_sample.append(second_mod_df_sample)
        dump_df_to_csv(sample_df, data_dir, data_filename.split(".")[0] +
                       "_sample_" + str(i) + ".csv")

    print("Split initial dataset in {} smaller balanced sets in {} min.".format(
          number_of_datasets_to_generate, round((time() - start) / 60)))
