# -*- coding: utf-8 -*-

"""
General utility functions to manipulate dataset
"""

import os
from typing import List, Tuple
import pandas as pd

def load_csv_from_lac_to_df(path: str, filename: str) -> pd.DataFrame:
    """Load csv file and return pandas dataframe.
    """
    df = pd.read_csv(os.path.join(path, filename), sep='|', encoding='latin1',
                     dtype='unicode', error_bad_lines=False)
    df.columns = [el.split('.')[1] for el in df.columns]

    return df

def load_csv_to_df(path: str, filename: str) -> pd.DataFrame:
    """Load csv file and return pandas dataframe.
    """
    df = pd.read_csv(os.path.join(path, filename), sep='|', dtype='unicode',
                     error_bad_lines=False)
    return df

def dump_df_to_csv(df: pd.DataFrame, path: str, filename: str):
    """Dump pandas df to csv file.
    """
    df.to_csv(os.path.join(path, filename), sep='|', index=False)

def build_train_valid_set(df: pd.DataFrame, split_percent: float) -> pd.DataFrame:
    """Make train and valid dataframes based on time (dd_datecreationreport).
    """
    df.sort_values('dd_datecreationreport', inplace=True)
    train_valid_split = int(len(df) * split_percent)
    train_df = df[:train_valid_split]
    valid_df = df[train_valid_split:]

    return train_df, valid_df
