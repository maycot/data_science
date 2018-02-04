import numpy as np
import pandas as pd
import time
from sklearn import linear_model
from load_datas import load_all_files
from regression import run_correlation_opti, run_greedy_opti
from flight_selection import select_time_flights, select_typical_flights
from threshold_opti import threshold_score, threshold_score2, threshold_value


fields = ["filename", "time", "phase", "static pressure", "air speed",
          "rotation speed", "outside temp", "n2", "n1", "torque",
          "turbine temp", "oil pressure", "oil temp", "fuel vol", "fuel flow",
          "ground speed", "altitude"]

lag_fields = ["phase", "static pressure", "air speed", "rotation speed",
              "outside temp", "n2", "n1", "turbine temp", "oil pressure",
              "oil temp", "fuel flow"]

N_LAG = 0
N_FEATURES = 10
lr = linear_model.LinearRegression()

all_datas = pd.read_csv("merged.csv")
all_datas.columns = fields

# Select typical flights
all_datas.reset_index(drop=True, inplace=True)
all_typical = select_time_flights(all_datas)
all_typical = select_typical_flights(all_typical)
all_typical.drop(["ground speed", "altitude"], axis=1, inplace=True)
# Renvoie 643 vols typiques

# Select features and torque threshold
train_y = all_typical["fuel flow"]
train_x = all_typical.drop("fuel flow", axis=1).copy()

greedy_scores, greedy_columns = run_greedy_opti(lr, N_FEATURES, train_x, train_y, train_x, train_y)

greedy_columns = ['torque'] + greedy_columns
threshold, threshold_results = threshold_value(5, 80, 5, lr, train_x, train_y, train_x, train_y,
                                               greedy_columns, "torque")

#####################################################
#####################################################
greedy_columns = ['torque', 'oil pressure', 'air speed','n2','oil temp',\
                    'outside temp','torque','rotation speed','n1','turbine temp']
threshold = 5

batch = 10
filename_list = list(np.unique(all_typical.filename))

T_max = 643
#MEAN
for t in range(0, 1000, batch):

    t_batch = t + batch

    train_files = filename_list[t: t_batch]
    test_files = filename_list[0:t] + filename_list[t_batch:]

    train_df = all_typical[(all_typical.filename.apply(lambda x: x in train_files))]
    train_df.reset_index(drop=True, inplace=True)

    test_df = all_typical[(all_typical.filename.apply(lambda x: x in test_files))]
    test_df.reset_index(drop=True, inplace=True)

    train_y = train_df["fuel flow"]
    train_x = train_df.drop("fuel flow", axis=1).copy()
    test_y = test_df["fuel flow"]
    test_x = test_df.drop("fuel flow", axis=1).copy()

    mean_res_batch = threshold_score2(lr, train_x, train_y, test_x, test_y,
                                         greedy_columns, "torque", threshold)

    mean_res_batch.to_csv("mean_res_batch_%i.csv"%t)

#VARIANCE
for t in range(0, T_max, batch):

    t_batch = t + batch

    train_files = filename_list[t: t_batch]
    test_files = filename_list[0:t] + filename_list[t_batch:]

    train_df = all_typical[(all_typical.filename.apply(lambda x: x in train_files))]
    train_df.reset_index(drop=True, inplace=True)

    test_df = all_typical[(all_typical.filename.apply(lambda x: x in test_files))]
    test_df.reset_index(drop=True, inplace=True)

    train_y = train_df["fuel flow"]
    train_x = train_df.drop("fuel flow", axis=1).copy()
    test_y = test_df["fuel flow"]
    test_x = test_df.drop("fuel flow", axis=1).copy()

    mean_res_batch = threshold_score3(lr, train_x, train_y, test_x, test_y,
                                         greedy_columns, "torque", threshold)

    mean_res_batch.to_csv("var_res_batch_%i.csv"%t)
