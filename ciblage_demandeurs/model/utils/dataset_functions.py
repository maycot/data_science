# -*- coding: utf-8 -*-

"""
General utility functions to manipulate dataset
"""

import os
import argparse
from typing import List, Tuple
import pandas as pd

def load_csv_from_lac_to_df(path: str, filename: str) -> pd.DataFrame:
    """Load csv file and return pandas dataframe.
    """
    df = pd.read_csv(os.path.join(path, filename), sep='|', dtype='str',
                     encoding='latin', error_bad_lines=False)
    df.columns = [el.split('.')[1] for el in df.columns]

    return df

def load_csv_to_df(path: str, filename: str) -> pd.DataFrame:
    """Load csv file and return pandas dataframe.
    """
    df = pd.read_csv(os.path.join(path, filename), sep='|')
    return df

def dump_df_to_csv(df: pd.DataFrame, path: str, filename: str):
    """Dump pandas df to csv file.
    """
    df.to_csv(os.path.join(path, filename), sep='|', index=False)

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("-r", "--rome", help="""Code Rome of the file""")
    return parser.parse_args()
