# -*- coding: utf-8 -*-

""" Create Dataset (features) for DNN model.
"""

import pandas as pd
import sys
sys.path.append('..')
import os
from sklearn.pipeline import Pipeline

from utils.dataset_utils import dump_df_to_csv, load_csv_to_df
from preprocess import Preprocess

path = '../data/'
input_filename = "input_file_lac.csv"
output_filename = "dataset_lac.csv"

def main():
    df = load_csv_to_df(path, input_filename)
    print(df.shape)

    pipeline = Pipeline([
        ('apply_rules', Preprocess())
        ])
    df = pipeline.fit_transform(df)

    dump_df_to_csv(df, path, output_filename)

if __name__ == '__main__':
    main()
