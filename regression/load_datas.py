from os import walk
import pandas as pd
import numpy as np
from scipy import stats
from sklearn import linear_model
from time import time


clean_path = "../clean/"

np.random.seed(0)

fields = ["filename", "time", "phase", "static pressure", "air speed",
          "rotation speed", "outside temp", "n2", "n1", "torque",
          "turbine temp", "oil pressure", "oil temp", "fuel vol", "fuel flow",
          "ground speed", "altitude"]

lag_fields = ["phase", "static pressure", "air speed", "rotation speed",
              "outside temp", "n2", "n1", "turbine temp", "oil pressure",
              "oil temp", "fuel flow"]

all_datas = pd.DataFrame(columns=fields)
n_lag = 0


def get_files_in_folder(path):
    results_list = []
    for filename in next(walk(path))[2]:
        results_list.append(filename)
    results_list.sort()
    return results_list


def load_all_files(path, df_all_datas, lag, window, to_load=False):
    if to_load is True:
        i = 0
        line = 0
        filename_list = get_files_in_folder(clean_path)
        for f in filename_list:
            if i % 1 == 0:
                if line % 50 == 0:
                    print(line, f)
                df = pd.read_csv(path + f, sep=",", header=0,
                                 encoding="latin 1")
                df["Unnamed: 0"] = f
                df.drop(["diff", "power"], axis=1, inplace=True)
                df = pd.DataFrame(df.values)
                df.columns = fields
                for c in lag_fields:
                    for n in range(1, lag + 1):
                        current_name = c + "_" + str(n)
                        df[current_name] = df[c].shift(n)
                columns = list(df.columns)
                columns.remove("filename")
                columns.remove("time")
                if window > 1:
                    df = df.rolling(window).mean()
                df_all_datas = df_all_datas.append(df)
                line += 1
            i += 1
        df_all_datas.dropna(inplace=True)
        df_all_datas.to_csv("merged.csv", index=False)
    else:
        df_all_datas = pd.read_csv("merged.csv")
        # df_all_datas.drop("Unnamed: 0", axis=1, inplace=True)
        df_all_datas.columns = fields
        filename_list = list(np.unique(df_all_datas.filename))
        # df_all_datas.drop(["power", "ground speed", "altitude"], axis=1, inplace=True)
    return df_all_datas, filename_list
