from regr_nico import *

from os import walk
import pandas as pd
import numpy as np
from scipy import stats
from sklearn import linear_model
from time import time

np.random.seed(42)

def build_datas_from_index2(df, columns, index_train_files, index_test_files):
    """Build train set : 100 random files
        Build test set : files from all_datas that are not in train set (350)"""

    train_x = df[columns].values[index_train_files]
    train_y = df["fuel flow"].values[index_train_files]
    test_x = df[columns].values[index_test_files]
    test_y = df["fuel flow"].values[index_test_files]
    return train_x, train_y, test_x, test_y


def make_train_test_index(n_files=100):

    list_filenames = list(set(all_datas["filename"].values))
    train_file = list(np.random.choice(list_filenames, size=n_files))
    test_file = [i for _,i in enumerate(list_filenames) if i not in train_file]

    index_train_files = [[i for i in np.where(all_datas["filename"].values == file)]\
                        for file in train_file]
    index_train_files = np.hstack(index_train_files).ravel()
    index_test_files = [[i for i in np.where(all_datas["filename"].values == file)] \
                        for file in test_file]
    index_test_files = np.hstack(index_test_files).ravel()

    return index_train_files, index_test_files

def run_greedy_opti2(clf, n_features, datas, index_train_files, index_test_files):

    columns = list(datas.columns)
    columns.remove("filename")
    columns.remove("time")
    columns.remove("fuel vol")
    columns.remove("fuel flow")
    scores = np.zeros([n_features, 2])
    used_columns = []
    min_mean_score = 10e10
    min_r2_score = 0.0
    min_columns = ""
    for i in range(n_features):
        print("=============== regression analysis ===============", "\n")
        print(i + 1, " ==========")
        min_mean_score = 10e10
        min_columns = ""
        for c in columns:
            train_x, train_y, test_x, test_y = \
                build_datas_from_index2(datas, used_columns + [c], \
                index_train_files, index_test_files)

            clf.fit(train_x, train_y)
            predictions = clf.predict(test_x)

            mean_error_score = mean_error(predictions, test_y)
            print(c, mean_error_score)
            if mean_error_score < min_mean_score:
                min_mean_score = mean_error_score
                min_r2_score = clf.score(test_x, test_y)
                min_columns = c
        print("\t", "===== ", min_columns, " =====")
        scores[i, 0] = round(min_r2_score * 100.0, 2)
        scores[i, 1] = round(min_mean_score * 100.0, 2)
        used_columns.append(min_columns)
        columns.remove(min_columns)
        print("\n")

    return scores, used_columns

def threshold_scores(clf, train_x, train_y, test_x, test_y, greedy_columns, \
                    threshold_col, threshold_value):
    """Compute regression mean residus for range of threshold values
    for a particular feature"""

    threshold_values = np.linspace(threshold_value - 10, threshold_value + 10, 5)
    threshold_mean_res = pd.DataFrame(columns=['mean_res', 'mean_res_diff',\
                        'train_size', 'test_size'], index = threshold_values)
    ind_col = [i for i,_ in enumerate(greedy_columns) if _ == threshold_col]

    for t in threshold_values :
        train_index_t = [i for i in range(len(train_x)) if train_x[i,ind_col] > t]
        test_index_t = [i for i in range(len(test_x)) if test_x[i,ind_col] > t]
        threshold_mean_res.ix[t, 'train_size'] = len(train_index_t)
        threshold_mean_res.ix[t, 'test_size'] = len(test_index_t)

        train_x = train_x[train_index_t]
        train_y = train_y[train_index_t]
        test_x = test_x[test_index_t]
        test_y = test_y[test_index_t]

        clf.fit(train_x, train_y)
        predictions = clf.predict(test_x)
        mean_res = mean_error(predictions, test_y)
        threshold_mean_res.ix[t, 'mean_res'] = round(mean_res * 100.0, 4)
    threshold_mean_res['mean_res_diff'] = -threshold_mean_res.mean_res.diff()

    return threshold_mean_res

start_time = time()
all_datas = load_all_files(clean_path, all_datas, n_lag, 0, to_load=True)
end_time = time()
print("file loaded in ", round(end_time - start_time, 2), " seconds", "\n")

index_train_files, index_test_files = make_train_test_index(n_files=100)
"""
train_x, train_y, test_x, test_y = build_datas_from_index2(all_datas, \
"torque",index_train_files, index_test_files)
print("size = train set : {} - test set : {}".format(train_x.shape[0], \
            test_x.shape[0]))
"""
n_iter = 10
lm = linear_model.LinearRegression()

#### Greddy Regression
greedy_scores, greedy_columns = run_greedy_opti2(lm, n_iter, all_datas, \
                                index_train_files, index_test_files)

train_x, train_y, test_x, test_y = build_datas_from_index(all_datas, \
                                greedy_columns, index_files)

print('========== Greedy Regression =======\n')
print(greedy_scores)
print('\ngreedy_columns :')
print(greedy_columns)

print('========== Threshold on "torque" ======\n')
threshold_val = 48 #(+ ou - 10)
thresh_col = "torque"
threshold_mean_res = threshold_scores(lm, train_x, train_y, test_x, test_y, \
                    greedy_columns, thresh_col, threshold_val)
print(threshold_scores)
