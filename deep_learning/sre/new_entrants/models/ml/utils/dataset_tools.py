# -*- coding: utf-8 -*-

import os
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sklearn.utils import shuffle

def load_csv_to_df(path, filename):
    """Load csv file and return pandas dataframe"""
    
    df = pd.read_csv(os.path.join(path, filename), sep=';', dtype='unicode')
    df.columns = [el.split(".")[1] for el in df.columns]
    return df


def dump_df_to_csv(df, path, filename):
    """Dump pandas df to csv file"""
    
    df.to_csv(os.path.join(path, filename), index=False)


def make_train_test_set(df, year_test, month_test, delta_train_set):
    """Make train and test set
    
    Parameters :
    ---
    year_test (int) ex: 2017 
    month_test (int) ex: 7
    delta_train_set (int) ex 4 (number of months for the train_set)
    """
    if not "datins0" in df.columns:
        df["datins"] = pd.to_datetime(df["datins"])
        df["datins0"] = df["datins"].values.astype("datetime64[M]")
        
    df["datins0"] = pd.to_datetime(df["datins0"])
    datins0_test = pd.Timestamp(datetime(year_test, month_test, 1))
    train_end = datins0_test - relativedelta(months=10)
    train_start = train_end - relativedelta(months=delta_train_set - 1)
    
    test_set = df[df["datins0"] == datins0_test]
    train_set = df[(df["datins0"] <= train_end) & (df["datins0"] >= train_start)]
    
    return shuffle(train_set), shuffle(test_set)