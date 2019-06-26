# -*- coding: utf-8 -*-

""" Predict SRE with DNN
"""

import numpy as np
import pandas as pd
from time import time
import train
from utils import dnn_utils, dataset_utils


def main():

    start = time()
    train_df  = dataset_utils.load_csv_to_df(train.data_dir,
                                             train.data_filename)

    test_df = pd.read_csv('./data/dataset_test_sample.csv')

    train_df = train_df[['ict1'] + train.emb_list +
                              train.no_emb_list].dropna()
    test_df = test_df[train.emb_list +
                      train.no_emb_list + ['bni']].dropna()

    model = train.build_graph(0.00001, False, train_df)
    pred = model.predict(test_df)
    test_df["sre"] = pred[0]

    dataset_utils.dump_df_to_csv(test_df, str(train.data_dir), 'pred' +
                                 '_' + str(train.name) + '.csv')

    print('Pred done in {} min.'.format(round((time() - start)
                                                     / 60)))


if __name__=='__main__':
    main()
