# -*- coding: utf-8 -*-

import sys
import os
import numpy as np
import pandas as pd
from datetime import datetime
from sklearn.utils import shuffle
from dateutil.relativedelta import relativedelta

def load_csv_to_df(path, filename):
    """Load csv file and return pandas dataframe.
    """
    df = pd.read_csv(os.path.join(path, filename), sep=';',
                     dtype='unicode')
    df.columns = [el.split(".")[1] for el in df.columns]
    return df


def dump_df_to_csv(df, path, filename):
    """Dump pandas df to csv file.
    """
    df.to_csv(os.path.join(path, filename), index=False)


def make_train_valid_set(data_dir, data_filename, year_valid,
                         month_valid_start, month_valid_end,
                         delta_train_set=None):
    """Make train and valid set

    Parameters :
    ---
    year_valid (int) ex: 2017
    month_valid (int) ex: 7
    """
    df = pd.read_csv(os.path.join(data_dir, data_filename), sep=',',
                     dtype='unicode')
    if not 'datins0' in df.columns:
        df['datins'] = pd.to_datetime(df['datins'])
        df['datins0'] = df['datins'].values.astype('datetime64[M]')

    df['datins0'] = pd.to_datetime(df['datins0'])
    datins0_valid_start = pd.Timestamp(datetime(year_valid,
                                                month_valid_start, 1))
    datins0_valid_end = pd.Timestamp(datetime(year_valid,
                                              month_valid_end, 1))
    train_end = datins0_valid_start - relativedelta(months=10)
    valid_set = df[(df['datins0'] >= datins0_valid_start) &
                   (df['datins0'] <= datins0_valid_end)]
    if delta_train_set:
        train_start = train_end - relativedelta(
            months=delta_train_set - 1)
        train_set = df[(df["datins0"] <= train_end) &
                       (df["datins0"] >= train_start)]
    else:
        train_set = df[df['datins0'] <= train_end]

    return shuffle(train_set), shuffle(valid_set)


def make_train_valid_set2(data_dir, data_filename, year_valid,
                         month_valid_start, month_valid_end, two_years):
    """Make train and valid set

    Parameters :
    ---
    year_valid (int) ex: 2017
    month_valid (int) ex: 7
    two_year (bool)
    """
    df = pd.read_csv(os.path.join(data_dir, data_filename), sep=',',
                     dtype='unicode')
    if not 'datins0' in df.columns:
        df['datins'] = pd.to_datetime(df['datins'])
        df['datins0'] = df['datins'].values.astype('datetime64[M]')

    df['datins0'] = pd.to_datetime(df['datins0'])
    datins0_valid_start = pd.Timestamp(datetime(year_valid,
                                                month_valid_start, 1))
    datins0_valid_end = pd.Timestamp(datetime(year_valid,
                                              month_valid_end, 1))
    valid_set = df[(df['datins0'] >= datins0_valid_start) &
                   (df['datins0'] <= datins0_valid_end)]

    datins0_train_start1 = datins0_valid_start - relativedelta(years=1)
    datins0_train_end1 = datins0_valid_end - relativedelta(years=1)
    train_set1 = df[(df["datins0"] <= datins0_train_end1) &
                    (df["datins0"] >= datins0_train_start1)]

    if two_years:
        datins0_train_start2 = datins0_valid_start - relativedelta(years=2)
        datins0_train_end2 = datins0_valid_end - relativedelta(years=2)
        train_set2 = df[(df["datins0"] <= datins0_train_end2) &
                        (df["datins0"] >= datins0_train_start2)]
        train_set = pd.concat([train_set1, train_set2])
        return shuffle(train_set), shuffle(valid_set)
    else:
        return shuffle(train_set1), shuffle(valid_set)
