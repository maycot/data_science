# -*- coding: utf-8 -*-

"""
Split quarter dataset in smaller and balanced (same ratio on type_pop feature)
datasets and write them to disk.
"""

import argparse
import pandas as pd

from utils2.dataset_utils import (load_csv_to_df, dump_df_to_csv, split_dataset)

data_dir = './data/'

feature_to_keep_ratio = "type_pop"
number_of_datasets_to_generate = 3

def main(filename):
    df = load_csv_to_df(data_dir, filename)
    split_dataset(df, feature_to_keep_ratio, number_of_datasets_to_generate,
                  data_dir, filename)

if __name__=='__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filename')
    filename = parser.parse_args().filename
    main(filename)
