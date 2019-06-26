# -*- coding: utf-8 -*-

import pandas as pd
from sklearn.preprocessing import LabelEncoder
from sklearn.base import BaseEstimator, TransformerMixin

class DataFrameSelectColumns(BaseEstimator, TransformerMixin):
    def __init__(self, col_list):
        self.col_list = col_list

    def fit(self, df):
        return self

    def transform(self, df):
        return df[self.col_list]

class DataFrameConvertNumeric(BaseEstimator, TransformerMixin):
    def fit(self, df):
        return self

    def transform(self, df):
        return df.astype("float")

class DataFrameConvertOneHotEncoding(BaseEstimator, TransformerMixin):
    def fit(self, df):
        return self

    def transform(self, df):
        return pd.concat([pd.get_dummies(df[col], prefix=col) for
                          col in df.columns], axis=1)

class DataFrameConvertLabelEncoding(BaseEstimator, TransformerMixin):
    def fit(self, df):
        return self

    def transform(self, df):
        return df.apply(lambda x :
                        LabelEncoder().fit_transform(x.tolist()))
