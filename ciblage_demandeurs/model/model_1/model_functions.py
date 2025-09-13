# -*- coding: utf-8 -*-

"""
Utility functions for model 1
"""

from typing import List
import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression

def filter_train_valid_sets_on_homogeneous_categ_features(
        categorical_feat_list: List[str], train_df: pd.DataFrame,
        valid_df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows in train and/or valid sets with uncommon categorical features.
    """
    print(f'Before filtering : train set shape: {train_df.shape}\
        / valid set shape: {valid_df.shape}')
    for categ in categorical_feat_list:
        union_list = list(set(train_df[categ]).intersection(valid_df[categ]))
        train_df = train_df[train_df[categ].isin(union_list)]
        valid_df = valid_df[valid_df[categ].isin(union_list)]
    print(f'After filtering : train set shape: {train_df.shape}\
        / valid set shape: {valid_df.shape}')

    return train_df, valid_df


def build_X_y_arrays(df: pd.DataFrame, target_name: str,
                     categorical_feat_list: List[str],
                     numerical_feat_list: List[str]) -> np.ndarray:
    """Build X and y arrays"""
    X = df[categorical_feat_list + numerical_feat_list]
    if target_name in df.columns:
        y = df[target_name].values
        return X, y

    return X

def preprocess(X_train: pd.DataFrame, X_valid: pd.DataFrame, full_pipeline):
    """Apply preprocessing on X arrays """
    X_train = full_pipeline.fit_transform(X_train)
    X_valid = full_pipeline.transform(X_valid)

    return X_train, X_valid


def train_random_forest_classifier(X_train: np.ndarray, y_train: np.ndarray):
    """Train Random Forest Classifier model."""
    regr = RandomForestClassifier(n_estimators=500, n_jobs=None, bootstrap=True,
                                  class_weight="balanced")

    regr_fit = regr.fit(X_train, y_train)

    return regr_fit

def train_logistic_regression_classifier(X_train: np.ndarray, y_train: np.ndarray):
    """Train Random Forest Classifier model."""
    regr = LogisticRegression(class_weight="balanced", solver='liblinear')

    regr_fit = regr.fit(X_train, y_train)

    return regr_fit

def predict(regr_fit, X_valid):
    """Make predictions on X_valid """
    y_pred = regr_fit.predict(X_valid)

    return y_pred
