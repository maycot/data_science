# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np
import operator
from sklearn_pandas import DataFrameMapper
from sklearn.preprocessing import LabelEncoder
from utils.prepare_input_for_embedding import create_features_training_time

def create_cat_var_dict(df, cat_var_list):
    """Create a dict with keys : categorical features and values : dimension of
    categorical features"""

    cat_var_dict = {}
    for cat_var in cat_var_list:
        cat_var_dict[cat_var] = len(set(df[cat_var]))

    return cat_var_dict

def encode_cat_var(df, cat_var_dict):
    """Encode categorical features : return the fit """

    cat_vars = [o[0] for o in sorted(cat_var_dict.items(),
                                     key=operator.itemgetter(1), reverse=True)]
    cat_maps = [(o, LabelEncoder()) for o in cat_vars]
    cat_mapper = DataFrameMapper(cat_maps)
    cat_map_fit = cat_mapper.fit(df)

    return cat_map_fit

def cat_preproc(cat_map_fit, df):
    """Encode categorical features : return the transform"""

    return cat_map_fit.transform(df).astype(np.int64)


def split_train_valid(df):
    train_end_day = str(df.iloc[-1].datetime - pd.Timedelta(days=2))[:10]
    df_train = df[df.datetime < train_end_day]
    df_valid = df[df.datetime >= train_end_day]

    return df_train, df_valid

def split_train_test(df_all, test_index, train_index):
    """Split df_all in train df and test df"""

    df_all.set_index("datetime", inplace=True)
    df_test = df_all.loc[test_index]
    df_train = df_all.loc[train_index]

    return df_train, df_test

def make_train_valid_array(df, cat_var_list):
    """Make the encoding for the train and valid set"""

    df = create_features_training_time(df)
    cat_var_dict = create_cat_var_dict(df, cat_var_list)
    cat_map_fit = encode_cat_var(df, cat_var_dict)
    df_train, df_valid = split_train_valid(df)

    cat_map_train = cat_preproc(cat_map_fit, df_train)
    cat_map_valid = cat_preproc(cat_map_fit, df_valid)

    y_train = df_train.cpu
    y_valid = df_valid.cpu

    return cat_map_fit, cat_map_train, y_train, cat_map_valid, y_valid

def make_train_test_array(df, cat_var_list, test_index, train_index):
    """Pipeline to make train and test array to feed the neural network model"""

    cat_var_dict = create_cat_var_dict(df, cat_var_list)
    cat_map_fit = encode_cat_var(df, cat_var_dict)
    df_train, df_test = split_train_test(df, test_index, train_index)
    cat_map_train = cat_preproc(cat_map_fit, df_train)
    cat_map_test = cat_preproc(cat_map_fit, df_test)
    y_train = df_train.cpu

    return cat_map_fit, cat_map_train, y_train, cat_map_test

def make_set_list(model_feat_set, *args):
    """Concat all the train and value sets in one list"""

    cat_var_list = []
    df_list = []
    for arg in args:
        if isinstance(arg, pd.DataFrame):
            df_list.append(arg)
    for cat in model_feat_set:
        cat_var_list.append(cat)

    cat_map_fit_list = []
    cat_map_train_list = []
    cat_map_valid_list = []
    y_train_list = []
    y_valid_list = []

    for df in df_list:
        cat_map_fit, cat_map_train, y_train, cat_map_valid, y_valid =\
        make_train_valid_array(df, cat_var_list)
        cat_map_fit_list.append(cat_map_fit)
        cat_map_train_list.append(cat_map_train)
        y_train_list.append(y_train)
        cat_map_valid_list.append(cat_map_valid)
        y_valid_list.append(y_valid)

    return cat_map_fit_list, cat_map_train_list, y_train_list, cat_map_valid_list, y_valid_list

