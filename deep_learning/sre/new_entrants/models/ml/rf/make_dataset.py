# -*- coding: utf-8 -*-

""" Create Dataset (features) - Random Forest Model and MLP
"""

import pandas as pd
import sys
sys.path.append('..')
import os
from sklearn.pipeline import Pipeline

from utils.dataset_tools import load_csv_to_df, dump_df_to_csv
from preprocess import Preprocess

path = '../data/'
input_filename = "input_file_lac_sep_dec.csv"
output_filename = "dataset_lac_sep_dec.csv"

def main():
    #df = pd.read_csv(os.path.join(path, input_filename), dtype='unicode')
    df = load_csv_to_df(path, input_filename)
    print(df.shape)

    pipeline = Pipeline([
        ('apply_rules', Preprocess())
        ])
    df = pipeline.fit_transform(df)

    dump_df_to_csv(df, path, output_filename)

if __name__ == '__main__':
    main()
