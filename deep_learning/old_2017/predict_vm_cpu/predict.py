# -*- coding: utf-8 -*-

############## Libraries ##############

import sys
import pandas as pd
import numpy as np
from keras.layers import merge
import datetime as dt


"""
Model 2

Changes from baseline model are:
    - features : add "isnan" in features
    - network layers :
        * baseline model : 6 layers with high number of neurons
        * model 2 : only 3 layers
    - dropout :
        * increase dropout for model 2
    - optimizer :
        * baseline model : ADAM
        * model 2 : Stockastic gradient descent optimizer SGD with decreasing
        learning rate
    - epochs number :
        * baseline model : 50
        * model 2 : 300
"""

data_path = "./data/"
result_path = "./preds/"

host = sys.argv[1]
data_filename = "cpu_" + host + ".csv"

############## Parameters ##############

epochs = 300
learning_rate = 0.005
decay_rate = learning_rate / epochs
momentum = 0.8
batch_size = 24
cat_var_list = ["month", "week", "day", "hour", "dweek", "isnan"]

############# My Functions ############

from utils.prepare_input_for_embedding import (read_df, create_features_predict_time)
from utils.make_embedding import (make_train_test_array)
from utils.network import (split_cols, get_emb, neural_network)

############

def main():

    df = read_df(data_path, data_filename)
    print("Check df size : {}".format(df.shape))
    df_all, train_index, test_index = create_features_predict_time(df)

    cat_map_fit, cat_map_train, y_train, cat_map_test = make_train_test_array(
        df_all, cat_var_list, test_index, train_index)

    map_train= split_cols(cat_map_train)
    map_test = split_cols(cat_map_test)

    embs = [get_emb(feat) for feat in cat_map_fit.features]

    x = merge([emb for inp, emb in embs], mode='concat')

    all_pred_df = pd.DataFrame(index=test_index)
    for i in range(30):
        print(i)
        model = neural_network(x, embs, momentum, learning_rate, decay_rate)
        hist = model.fit(map_train, y_train, batch_size=batch_size, epochs=epochs,
                         verbose=0)
        pred = np.squeeze(model.predict(map_test))
        pred_df = pd.DataFrame(pred, index=test_index, columns=["cpu_pred"])
        all_pred_df = pd.concat([all_pred_df, pred_df], axis=1)

    final_pred = pred_df.mean(axis=1)
    final_pred.to_csv(result_path + "pred_" + host + "_" + str(dt.date.today())
                      + ".csv")

if __name__ == '__main__':
    main()
