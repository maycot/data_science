# -*- coding: utf-8 -*-

""" Functions that prepare input for embeddings
    Input : csv files with columns "timestamp" and "cpu"
"""

import pandas as pd
import numpy as np

def read_df(path, filename):
    df = pd.read_csv(path + filename, sep="|")
    df.datetime = pd.to_datetime(df.datetime)
    df = df.sort_values("datetime")
    df.set_index("datetime", inplace=True)
    df = df.resample("h").mean()
    return df

def add_datepart(df):
    #df["year"] = df.datetime.dt.year
    df["month"] = df.datetime.dt.month
    df["week"] = df.datetime.dt.week
    df["day"] = df.datetime.dt.day
    df["hour"] = df.datetime.dt.hour
    return df

def fill_nan_values(df):
    """Fill nan values with the hourly mean values for the same day week of the
    same month
    Input : df with columns 'month', 'dweek' and 'isnan' indicating the rows
    that are nan values
    """
    dweek_mean_values = df.groupby(["month", "dweek", "hour"]).cpu.mean()
    for month in list(set(df.month)):
        for dweek in list(set(df.dweek)):
            for hour in list(set(df.hour)):
                if len(dweek_mean_values.loc[
                    (dweek_mean_values.index.get_level_values('month') == month)
                    & (dweek_mean_values.index.get_level_values('dweek') == dweek)
                    & (dweek_mean_values.index.get_level_values('hour') == hour)])> 0:

                    df = df.set_value(df[(df["isnan"] == 1) &
                                         (df["month"] == month) &
                                         (df["dweek"] == dweek) &
                                         (df["hour"] == hour)].index,
                                      "cpu",
                                      dweek_mean_values.loc[
                                          (dweek_mean_values.index.get_level_values('month') == month) &
                                          (dweek_mean_values.index.get_level_values('dweek') == dweek) &
                                          (dweek_mean_values.index.get_level_values('hour') == hour)].values[0])
    return df

def create_features_predict_time(df):
    """Create features for the time series"""

    test_start = str(df.iloc[-1].name + pd.Timedelta(days=1))[:10]
    rng = pd.date_range(test_start, periods=24*5, freq='H')
    df_test = pd.DataFrame(np.zeros((len(rng),1)), index = rng,
                           columns = ["cpu"])
    df_all = df.append(df_test)
    # Create days of week features
    df_all["dweek"] = df_all.index.dayofweek
    df_all.reset_index(inplace=True)
    df_all = df_all.rename(columns = {'index':'datetime'})
    # Create features : month, day, hour
    df_all = add_datepart(df_all)
    # Create feature weekend
    df_all["weekend"] = 0
    df_all.loc[df_all[df_all.dweek > 4].index, "weekend"] = 1
    # Create feature isnan
    df_all["isnan"] = 0
    df_all.loc[df_all[df_all.cpu.isnull()].index, "isnan"] = 1
    # Fill nan values
    df_all = fill_nan_values(df_all)

    return df_all, df.index, df_test.index

def create_features_training_time(df):
    """Create features for the time series"""

    # Create days of week features
    df["dweek"] = df.index.dayofweek
    df.reset_index(inplace=True)
    # Create features : month, day, hour
    df = add_datepart(df)
    # Create feature weekend
    df["weekend"] = 0
    df.loc[df[df.dweek > 4].index, "weekend"] = 1
    # Create feature isnan
    df["isnan"] = 0
    df.loc[df[df.cpu.isnull()].index, "isnan"] = 1
    # Fill nan values
    df = fill_nan_values(df)

    return df
