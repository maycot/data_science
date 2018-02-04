from os import walk
import pandas as pd
import numpy as np
from scipy import stats
from sklearn import linear_model
from time import time
from load_datas import load_all_files

clean_path = "../clean/"

np.random.seed(0)

fields = ["filename", "time", "phase", "static pressure", "air speed",
          "rotation speed", "outside temp", "n2", "n1", "torque",
          "turbine temp", "oil pressure", "oil temp", "fuel vol", "fuel flow",
          "power", "ground speed", "altitude"]

lag_fields = ["phase", "static pressure", "air speed", "rotation speed",
              "outside temp", "n2", "n1", "turbine temp", "oil pressure",
              "oil temp", "fuel flow"]

all_datas = pd.DataFrame(columns=fields)
n_lag = 0


def mean_error(predictions, y):
    y_without0 = y.copy()
    y_without0[np.where(y_without0 == 0)[0]] = 10e15
    residus = np.abs(predictions - y_without0)
    error = residus / y_without0
    mean_err = np.mean(error)
    return mean_err

def mean_error2(predictions, y):
    y_without0 = y.copy()
    y_without0[np.where(y_without0 == 0)[0]] = 10e15
    residus = predictions - y_without0
    error = residus 
    mean_err = np.mean(error)
    return mean_err

def std_error(predictions, y):
    y_without0 = y.copy()
    y_without0[np.where(y_without0 == 0)[0]] = 10e15
    residus = predictions - y_without0
    error = residus / y_without0
    std_err = np.std(error)
    return std_err

def build_datasets(df, columns):
    train_x = df[columns].values
    train_y = df["fuel flow"].values
    test_x = df[columns].values
    test_y = df["fuel flow"].values
    return train_x, train_y, test_x, test_y


def run_correlation_opti(clf, n_features, train_x, train_y, test_x, test_y):
    columns = list(train_x.columns)  # fields[2:-3]
    columns.remove("filename")
    columns.remove("time")
    columns.remove("fuel vol")
    columns.remove("fuel flow")
    columns.remove("torque")
    scores = np.zeros([n_features + 1, 2])
    used_columns = ["torque"]
    correlations = np.zeros([n_features, len(columns)])

    for i in range(n_features + 1):

        current_train_x = train_x[used_columns]
        current_test_x = test_x[used_columns]

        clf.fit(current_train_x, train_y)

        predictions = clf.predict(current_test_x)
        scores[i, 0] = round(clf.score(current_test_x, test_y) * 100, 2)
        residus = test_y - predictions
        scores[i, 1] = round(mean_error(predictions, test_y) * 100, 2)

        if i <= n_features - 1:
            for j in range(len(columns)):
                correlations[i, j] = np.abs(
                    stats.pearsonr(residus, test_x[columns[j]])[0])

            index_best_correl = np.argsort(correlations[i, :])[-1]
            used_columns.append(columns[index_best_correl])
            columns.remove(columns[index_best_correl])

    return scores, used_columns


def run_greedy_opti(clf, n_features, train_x, train_y, test_x, test_y):
    columns = list(train_x.columns)
    columns.remove("filename")
    columns.remove("time")
    columns.remove("fuel vol")
    used_columns = []

    scores = np.zeros([n_features + 1, 2])
    min_r2_score = 0.0
    for i in range(n_features + 1):
        min_mean_score = 10e10
        min_columns = ""
        for c in columns:
            current_train_x = train_x[used_columns + [c]]
            current_test_x = test_x[used_columns + [c]]

            clf.fit(current_train_x, train_y)

            predictions = clf.predict(current_test_x)
            mean_error_score = mean_error(predictions, test_y)

            if mean_error_score < min_mean_score:
                min_mean_score = mean_error_score
                min_r2_score = clf.score(current_test_x, test_y)
                min_columns = c

        if i > 0 and min_mean_score * 100.0 > scores[i - 1, 1]:
            break

        scores[i, 0] = round(min_r2_score * 100.0, 2)
        scores[i, 1] = round(min_mean_score * 100.0, 2)
        used_columns.append(min_columns)
        columns.remove(min_columns)

    return scores, used_columns
