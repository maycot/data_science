# -*- coding: utf-8 -*-

""" Apply Random Forest to dataset
"""

import sys
sys.path.append('..')
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from time import time
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import roc_auc_score, accuracy_score
from sklearn.pipeline import Pipeline, FeatureUnion

from utils.pipeline_preprocess_class import (DataFrameSelectAreas,
                                             DataFrameSelectColumns,
                                             DataFrameConvertNumeric,
                                             DataFrameConvertOneHotEncoding)


def apply_algo(n_estimators, max_depth, n_jobs,
               graph, num_list, cat_list, df_train, df_test):
    
    start = time()
    # 2 different pipelines if cat_list or not
    if len(cat_list) >= 1:
        num_pipeline = Pipeline([
            ('col_selector', DataFrameSelectColumns(num_list)),
            ('num_encoder', DataFrameConvertNumeric())
        ])
    
        cat_pipeline = Pipeline([
            ('col_selector', DataFrameSelectColumns(cat_list)),
            ('ohe_encoder', DataFrameConvertOneHotEncoding())
        ])
        
        full_pipeline = FeatureUnion(transformer_list=[
            ('num_pipeline', num_pipeline),
            ('cat_pipeline', cat_pipeline)
        ])
    
        df_train_ = full_pipeline.fit_transform(df_train)
        df_test_ = full_pipeline.transform(df_test)
        y_train = df_train_[:, 0]
        X_train = df_train_[:, 1:]
        y_test = df_test_[:, 0]
        X_test = df_test_[:, 1:]
        
    else:
        num_pipeline = Pipeline([
            ('col_selector', DataFrameSelectColumns(num_list)),
            ('num_encoder', DataFrameConvertNumeric())
        ])
        df_train_ = num_pipeline.fit_transform(df_train)
        df_test_ = num_pipeline.transform(df_test)
        y_train = df_train_.iloc[:, 0]
        X_train = df_train_.iloc[:, 1:]
        y_test = df_test_.iloc[:, 0]
        X_test = df_test_.iloc[:, 1:]
        
    clf = RandomForestClassifier(n_estimators=n_estimators,
                                 n_jobs=n_jobs, bootstrap=True,
                                 class_weight="balanced_subsample")
    
    clf_fit = clf.fit(X_train, y_train)
    predict_proba = clf_fit.predict_proba(X_test)[:,1]
    pred = clf_fit.predict(X_test)

    print("\nDone in min : {}".format((time() - start) / 60))
    
    auc = roc_auc_score(y_test, predict_proba)
    acc_score = accuracy_score(y_test, pred)
    
    print("\nauc : {}\naccuracy : {}".format(auc, acc_score))
    
    if graph:
        # Find columns names to plot importance features
        if len(cat_list) >= 1:
            df1 = num_pipeline.fit_transform(df_test)
            col1 = df1.iloc[:, 1:].columns
            df2 = cat_pipeline.fit_transform(df_test)
            col2 = df2.columns
            col = list(col1) + list(col2)
        else:
            df1 = num_pipeline.fit_transform(df_test)
            col = df1.iloc[:, 1:].columns
    
        importances = clf.feature_importances_
        std = np.std([tree.feature_importances_ 
                      for tree in clf.estimators_],
                     axis=0)
        indices = np.argsort(importances)[::-1]

        # Print the feature ranking
        print("\nFeature ranking:")

        for f in range(X_train.shape[1]):
            print("%d. feature %d %s (%f)" % (f + 1, indices[f],
                                              col[int(indices[f])],
                                              importances[indices[f]]))

        # Plot the feature importances of the forest
        plt.figure(figsize=(20, 5))
        plt.title("Feature importances")
        plt.bar(range(X_train.shape[1]), importances[indices],
               color="r", yerr=std[indices], align="center")
        plt.xticks(range(X_train.shape[1]), indices)
        plt.xlim([-1, X_train.shape[1]])
        plt.show()
