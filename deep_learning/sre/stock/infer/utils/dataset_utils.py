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