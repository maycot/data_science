# -*- coding: utf-8 -*-

import pandas as pd
import numpy as np

import keras
import keras.backend as K
from keras.models import Model
from keras.layers import merge, Input, Dense, Dropout
from keras import initializers
from keras.layers import Flatten, Embedding
from keras.optimizers import SGD

def rmspe(y_pred, y_true):
    """Compute root-mean-squared percent error"""

    pct_var = (y_true - y_pred)/y_true
    return math.sqrt(np.square(pct_var).mean())

def split_cols(arr):
    return np.hsplit(arr, arr.shape[1])


def cat_map_info(feat):
    """Return categ features dimensions"""

    return feat[0], len(feat[1].classes_)

def get_emb(feat):
    name, c = cat_map_info(feat)
    c2 = (c+1) // 2
    inp = Input((1,), dtype='int64', name=name+'_in')
    u = Flatten(name=name+'_flt')(Embedding(c, c2, input_length=1)(inp))
    return inp, u


def neural_network(x, embs, momentum, learning_rate, decay_rate):
    """Neural network layers"""

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
