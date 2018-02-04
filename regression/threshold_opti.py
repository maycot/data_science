import numpy as np
import pandas as pd
from regression import mean_error, mean_error2, std_error


def threshold_score(clf, train_x, train_y, test_x, test_y, greedy_columns, thresh_col, thresh_value):
    """Compute regression mean residus for a threshold value for a feature"""

    train_index_sup = np.where(train_x[thresh_col] >= thresh_value)[0]
    train_index_inf = np.where(train_x[thresh_col] < thresh_value)[0]
    test_index_sup = np.where(test_x[thresh_col] >= thresh_value)[0]
    test_index_inf = np.where(test_x[thresh_col] < thresh_value)[0]

    train_x_sup = train_x[greedy_columns].values[train_index_sup, :]
    train_x_inf = train_x[greedy_columns].values[train_index_inf, :]
    train_y_sup = train_y.values[train_index_sup]
    train_y_inf = train_y.values[train_index_inf]

    test_x_sup = test_x[greedy_columns].values[test_index_sup, :]
    test_x_inf = test_x[greedy_columns].values[test_index_inf, :]
    test_y_sup = test_y.values[test_index_sup]
    test_y_inf = test_y.values[test_index_inf]

    clf.fit(train_x_sup, train_y_sup)
    predictions1 = clf.predict(test_x_sup)
    clf.fit(train_x_inf, train_y_inf)
    predictions2 = clf.predict(test_x_inf)

    predictions = np.concatenate((predictions1, predictions2), axis=0)
    test_y = np.concatenate((test_y_sup, test_y_inf), axis=0)
    mean_res = mean_error(predictions, test_y)

    return round(mean_res * 100, 2), predictions


def threshold_value(tinf, tmax, step, clf, train_x, train_y, test_x, test_y,
                    greedy_columns, thresh_col):
    """Compute the minimum mean residu for a range of threshold values"""

    thresh_value_list = np.arange(tinf, tmax, step)
    thresh_results = pd.DataFrame(columns=['mean_res'], index=thresh_value_list)

    for thresh_value in thresh_value_list:
        mean_res, _ = threshold_score(clf, train_x, train_y, test_x, test_y, greedy_columns, thresh_col, thresh_value)
        thresh_results.ix[thresh_value, 'mean_res'] = mean_res

    return thresh_results.mean_res.argmin(), thresh_results


def threshold_score2(clf, train_x, train_y, test_x, test_y, greedy_columns, thresh_col, thresh_value):
    """Compute regression mean residus by flight for a threshold value for a feature"""

    train_index_sup = np.where(train_x[thresh_col] >= thresh_value)[0]
    train_index_inf = np.where(train_x[thresh_col] < thresh_value)[0]
    test_index_sup = np.where(test_x[thresh_col] >= thresh_value)[0]
    test_index_inf = np.where(test_x[thresh_col] < thresh_value)[0]

    train_x_sup = train_x[greedy_columns].values[train_index_sup, :]
    train_x_inf = train_x[greedy_columns].values[train_index_inf, :]
    train_y_sup = train_y.values[train_index_sup]
    train_y_inf = train_y.values[train_index_inf]

    test_x_sup = test_x[greedy_columns].values[test_index_sup, :]
    test_x_inf = test_x[greedy_columns].values[test_index_inf, :]
    test_y_sup = test_y.values[test_index_sup]
    test_y_inf = test_y.values[test_index_inf]

    clf.fit(train_x_sup, train_y_sup)
    predictions1 = clf.predict(test_x_sup)
    clf.fit(train_x_inf, train_y_inf)
    predictions2 = clf.predict(test_x_inf)

    predictions = pd.DataFrame(columns = ['pred'], index=test_x.index)
    predictions.ix[test_index_inf, 'pred'] = predictions2
    predictions.ix[test_index_sup, 'pred'] = predictions1

    flight_list = np.unique(test_x.filename)
    mean_res_flight = pd.DataFrame(columns=['res_flight'],index=flight_list)

    for flight in flight_list:
        ind = test_x[test_x.filename == flight].index
        mean_res_flight.ix[flight, 'res_flight'] = mean_error2(predictions.ix[ind, 'pred'].values, test_y.ix[ind].values)

    return mean_res_flight

def threshold_score3(clf, train_x, train_y, test_x, test_y, greedy_columns, thresh_col, thresh_value):
    """Compute regression variance residus by flight for a threshold value for a feature"""

    train_index_sup = np.where(train_x[thresh_col] >= thresh_value)[0]
    train_index_inf = np.where(train_x[thresh_col] < thresh_value)[0]
    test_index_sup = np.where(test_x[thresh_col] >= thresh_value)[0]
    test_index_inf = np.where(test_x[thresh_col] < thresh_value)[0]

    train_x_sup = train_x[greedy_columns].values[train_index_sup, :]
    train_x_inf = train_x[greedy_columns].values[train_index_inf, :]
    train_y_sup = train_y.values[train_index_sup]
    train_y_inf = train_y.values[train_index_inf]

    test_x_sup = test_x[greedy_columns].values[test_index_sup, :]
    test_x_inf = test_x[greedy_columns].values[test_index_inf, :]
    test_y_sup = test_y.values[test_index_sup]
    test_y_inf = test_y.values[test_index_inf]

    clf.fit(train_x_sup, train_y_sup)
    predictions1 = clf.predict(test_x_sup)
    clf.fit(train_x_inf, train_y_inf)
    predictions2 = clf.predict(test_x_inf)

    predictions = pd.DataFrame(columns = ['pred'], index=test_x.index)
    predictions.ix[test_index_inf, 'pred'] = predictions2
    predictions.ix[test_index_sup, 'pred'] = predictions1

    flight_list = np.unique(test_x.filename)
    std_res_flight = pd.DataFrame(columns=['res_flight'],index=flight_list)

    for flight in flight_list:
        ind = test_x[test_x.filename == flight].index
        std_res_flight.ix[flight, 'res_flight'] = std_error(predictions.ix[ind, 'pred'].values, test_y.ix[ind].values)

    return std_res_flight
