from os import walk
import pandas as pd
import numpy as np
from scipy import stats
from sklearn import linear_model
from time import time

# init 1
np.random.seed(0)
# init 2
#np.random.seed(42)

fields = ["filename", "time", "phase", "static pressure", "air speed",
          "rotation speed", "outside temp", "n2", "n1", "torque",
          "turbine temp", "oil pressure", "oil temp", "fuel vol", "fuel flow",
          "power", "ground speed", "altitude"]



lr = linear_model.LinearRegression()
N_FEATURES = 10


def mean_error(predictions, y):
    y_without0 = y.copy()
    y_without0[np.where(y_without0 == 0)[0]] = 10e15
    residus = np.abs(predictions - y_without0)
    error = residus / y_without0
    mean_err = np.mean(error)
    return mean_err


def run_greedy_opti(clf, n_features, train_x, train_y, test_x, test_y):

    """
     arg:
     clf = model
     n_feature = nb_max de feature à parcourir/nb_max de colonne en retour

     return:
     score= r**2 et mean_score
     greedy_columns= greedy features
    """
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
        for idx,c in enumerate(columns):
            current_train_x = train_x[used_columns + [c]]
            current_test_x = test_x[used_columns + [c]]

            clf.fit(current_train_x, train_y)

            predictions = clf.predict(current_test_x)

            mean_error_score = mean_error(predictions, test_y)

            if mean_error_score < min_mean_score:
                min_mean_score = mean_error_score
                min_r2_score = clf.score(current_test_x, test_y)
                min_columns = c

        scores[i, 0] = round(min_r2_score * 100.0, 2)
        scores[i, 1] = round(min_mean_score * 100.0, 2)
        used_columns.append(min_columns)
        columns.remove(min_columns)

    return scores, used_columns

def compute_weight(greedy_columns, train_x, train_y, val_x):
    """
    input:
    greedy columns: colonne greedy
    trainx: dataset sans fuel flow
    train_y = fuel flow
    val_x : validationset dans fuel flow

    return dataframe des poids
    """

    # store poids
    w = len(greedy_columns)
    matrix_weight = np.zeros((w,w))
    # store intercept
    liste_intercept=[]

    used_columns=[]
    for idx, c in enumerate(greedy_columns):

        current_train_x = train_x[used_columns + [c]]
        current_test_x = val_x[used_columns + [c]]

        current_feature = [used_columns + [c]]
        clf= linear_model.LinearRegression()
        clf.fit(current_train_x, train_y)
        prediction=clf.predict(current_test_x)
        coef = clf.coef_
        intercept = clf.intercept_
        liste_intercept.append(intercept)
#         print(len(current_feature[0]), current_feature, coef)
        for i in range(len(current_feature[0])):
            poids = np.sum(coef[i]*current_test_x[current_test_x.columns[i]]) / np.sum(prediction)
#             print('coef' ,coef[i], 'poids',poids)
            matrix_weight[i,idx]=poids
        used_columns.append(c)

    matrix = pd.DataFrame(matrix_weight,columns= greedy_columns)
    return matrix.append(pd.DataFrame(liste_intercept, index=matrix.columns, columns=['intercept']).transpose( ))


def compute_lag(df, lag,lag_fields):
    '''
    arg:
    df = dataframe
    lag= fenetre de lag
    lag_field = colonne a lagué

    return df lagué
    '''
    liste_columns=[]
    for l in range(1, lag+1):
        tdf_tmp = df[lag_fields].shift(l).copy()
        new_name_columns = [i+'_'+str(l) for i in tdf_tmp.columns]
        tdf_tmp.columns = new_name_columns
        df = pd.concat([df, tdf_tmp], axis=1)
    liste_columns=list(df.columns)
    if('fuel flow' in liste_columns):
        liste_columns.remove('fuel flow')
    return df[liste_columns].dropna()


def make_train_test_index(df, N):
    """return index for train files and test files"""

    list_filenames = sorted(list(set(df["filename"].values)))
    train_file = np.random.choice(list_filenames, N)
    test_file = [i for _,i in enumerate(list_filenames) if i not in train_file]

    return train_file, test_file


def build_data(df, columns, seuil, direction, train_file, test_file):
    """
    train : 300 fichiers aléatoires / test : 1049 fichiers restants
    seuil torque à 30
    df= dataframe
    columns= colonne à selectionner
    seuil= seuil du torque
    direction = sup/inf

    """
    train_x_ = df[(df.filename.apply(lambda x: x in train_file))]
    val_x_ = df[(df.filename.apply(lambda x: x in test_file))]

    if(direction =='sup'):
        train_x = train_x_[train_x_.torque > seuil][columns].drop("fuel flow", axis=1)
        train_y = train_x_[train_x_.torque > seuil]["fuel flow"]
        val_x = val_x_[val_x_.torque > seuil][columns].drop("fuel flow", axis=1)
        val_y = val_x_[val_x_.torque > seuil]["fuel flow"]

    if( direction =='inf'):
        train_x = train_x_[train_x_.torque < seuil][columns].drop("fuel flow", axis=1)
        train_y = train_x_[train_x_.torque < seuil]["fuel flow"]
        val_x = val_x_[val_x_.torque < seuil][columns].drop("fuel flow", axis=1)
        val_y = val_x_[val_x_.torque < seuil]["fuel flow"]

    return train_x, train_y, val_x, val_y

def compute_tableau(datas_, columns_, seuil, direction):
    train_file, test_file = make_train_test_index(datas_, 300)
    train_x, train_y, val_x, val_y = build_data(datas_, columns_, seuil, direction, train_file, test_file)
    greedy_scores, greedy_columns = run_greedy_opti(lr, N_FEATURES, train_x, train_y, val_x, val_y)
    matrix = compute_weight(greedy_columns,train_x, train_y, val_x)
    return matrix, greedy_columns, greedy_scores, greedy_columns



columns_= ["filename", "time", "phase", "static pressure", "air speed",
          "rotation speed", "outside temp", "n2", "n1", "torque",
          "turbine temp", "oil pressure", "oil temp", "fuel vol", "fuel flow",
            "ground speed", "altitude"]

def compute_tableau_lag(datas_, columns_, seuil, direction, lag, lag_fields):

    train_file, test_file = make_train_test_index(datas_, 300)
    train_x, train_y, val_x, val_y = build_data(datas_, columns_, seuil, direction, train_file, test_file)
    trained_lag_x = compute_lag(train_x, lag, lag_fields)
    trained_lag_y = train_y[lag:]
    val_lag_y = val_y[lag:]
    val_lag_x = compute_lag(val_x, lag,lag_fields)

    greedy_scores, greedy_columns = run_greedy_opti(lr, N_FEATURES, trained_lag_x, trained_lag_y, val_lag_x, val_lag_y)
    matrix = compute_weight(greedy_columns, trained_lag_x, trained_lag_y, val_lag_x)

    return matrix, greedy_columns, greedy_scores, greedy_columns

lag_fields = ["phase", "static pressure", "air speed", "rotation speed",
              "outside temp", "n2", "n1", "torque", "turbine temp", "oil pressure",
              "oil temp",   "ground speed", "altitude"]


########### GENERATION DES TABLEAUX #########################################
all_datas = pd.read_csv("merged.csv")
# without lag, sup threshold
matrix, greedy_columns, greedy_scores, greedy_columns = compute_tableau(all_datas, columns_, 30, 'sup')
matrix2 = matrix.round(decimals=3)
matrix2.to_csv("bis_seuil_30_no_lag.csv")
# without lag, without threshold
matrix, greedy_columns, greedy_scores, greedy_columns = compute_tableau(all_datas, columns_, 0, 'sup')
matrix2 = matrix.round(decimals=3)
matrix2.to_csv("bis_seuil_0_no_lag.csv")

# with lag 1 to 5, seuil 30
lag = 5
matrix, greedy_columns, greedy_scores, greedy_columns =  compute_tableau_lag(all_datas, columns_, 30, 'sup', lag, lag_fields)
matrix2 = matrix.round(decimals=3)
matrix2.to_csv("bis_seuil_30_lag.csv")

matrix, greedy_columns, greedy_scores, greedy_columns =  compute_tableau_lag(all_datas, columns_, 0, 'sup', lag, lag_fields)
matrix2 = matrix.round(decimals=3)
matrix2.to_csv("bis_no_seuil_lag.csv")

greedy_scores = pd.DataFrame(greedy_scores)
greedy_scores = greedy_scores.round(decimals=2)
greedy_scores.to_csv("greedy_scores12.csv")
