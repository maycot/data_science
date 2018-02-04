# -*- coding: utf-8 -*-

############## Libraries ##############

import sys
import pandas as pd
import numpy as np

import keras
import keras.backend as K
from keras.models import Model
from keras.layers import merge, Input, Dense, Dropout
from keras import initializers
from keras.layers import Flatten, Embedding

""" Test different models on 8 different train_valid set """


data_path = "./data/"

host = "slzq0b"
data_filename = "cpu_" + host + ".csv"

############## Paramters ##############

epochs = 300
learning_rate = 0.005
decay_rate = learning_rate / epochs
momentum = 0.8
batch_size = 24
cat_var_list = ["month", "week", "day", "hour", "dweek", "isnan"]

############# My Functions ############

from utils.prepare_input_for_embedding import (read_df, create_features_training_time)
from utils.make_embedding import (make_train_valid_array, make_set_list)
from utils.network import (split_cols, get_emb, rmspe)

############

def make_batch(first_batch_end_date = "2017-11-29"):

    df = read_df(data_path, data_filename)
    # Prepare the different sets
    df1 = df.loc[:first_batch_end_date]
    df2 = df1.tshift(periods=1, freq="D", axis=0)
    df3 = df2.tshift(periods=1, freq="D", axis=0)
    df4 = df3.tshift(periods=1, freq="D", axis=0)
    df5 = df4.tshift(periods=1, freq="D", axis=0)
    df6 = df5.tshift(periods=1, freq="D", axis=0)
    df7 = df6.tshift(periods=1, freq="D", axis=0)
    df8 = df7.tshift(periods=1, freq="D", axis=0)

    cat_var_list = ["month", "week", "day", "hour", "dweek", "isnan", "weekend"]
    cat_map_fit_list, cat_map_train_list, y_train_list, cat_map_valid_list,\
        y_valid_list = make_set_list(cat_var_list, df1, df2, df3, df4, df5, df6,
                                 df7, df8)

    return cat_map_fit_list, cat_map_train_list, y_train_list, cat_map_valid_list, y_valid_list

############# The common parameters ############

epochs = 300
learning_rate = 0.01
decay_rate = learning_rate / epochs
momentum = 0.8

############# Models to test ###################

def neural_network(x, embs, model_nb):

    if model_nb == 0:

        x = Dropout(0.1)(x)
        x = Dense(20, init='uniform')(x)
        x = Dense(100, activation='elu', init='uniform')(x)
        x = Dropout(0.2)(x)
        x = Dense(100, activation='elu', init='uniform')(x)
        x = Dense(20, init='uniform')(x)
        x = Dense(100, activation='elu', init='uniform')(x)
        x = Dense(1)(x)
        model = Model([inp for inp,emb in embs], x)
        sgd = SGD(lr=learning_rate, momentum=momentum, decay=decay_rate, nesterov=False)
        model.compile(optimizer=sgd, loss='mean_absolute_error')

        return model

    if model_nb == 1:

        x = Dropout(0.1)(x)
        x = Dense(200, activation='elu', init='uniform')(x)
        x = Dense(500, activation='elu', init='uniform')(x)
        x = Dropout(0.2)(x)
        x = Dense(300, activation='elu', init='uniform')(x)
        x = Dense(1)(x)
        model = Model([inp for inp,emb in embs], x)
        sgd = SGD(lr=learning_rate, momentum=momentum, decay=decay_rate, nesterov=False)
        model.compile(optimizer=sgd, loss='mean_absolute_error')

        return model

############ Test models on the 8 train_valid sets #############

cat_map_fit_list, cat_map_train_list, y_train_list, cat_map_valid_list,y_valid_list = make_batch(first_batch_end_date = "2017-11-29")

models_metrics_list = []

for model_nb in [0, 1]:

    model_metrics = []

    for set_number in range(len(cat_map_fit_list)):

        cat_map_fit = cat_map_fit_list[set_number]
        cat_map_train = cat_map_train_list[set_number]
        y_train =  y_train_list[set_number]
        cat_map_valid = cat_map_valid_list[set_number]
        y_valid = y_valid_list[set_number]

        map_train= split_cols(cat_map_train)
        map_valid = split_cols(cat_map_valid)

        embs = [get_emb(feat) for feat in cat_map_fit.features]
        x = merge([emb for inp, emb in embs], mode='concat')
        model = neural_network(x, embs, model_nb)

        hist = model.fit(map_train, y_train, batch_size=50, epochs=500, verbose=0, validation_data=(map_valid, y_valid))

        preds = np.squeeze(model.predict(map_valid))
        model_metrics.append(rmspe(preds, y_valid))
    print("mean error for the 8 sets : {}.".format(model_metrics))

    model_mean = [np.mean(model_metrics)]
    model_std = [np.std(model_metrics)]
    models_metrics_list.append(model_mean + model_std)
    print(models_metrics_list)
