
# -*- coding: utf-8 -*-

"""
Create Dataset (features)
"""

from time import time
from sklearn.pipeline import Pipeline
from utils.dataset_functions import load_csv_from_lac_to_df, dump_df_to_csv
from pourvoi.preprocess import Preprocess

PATH = './data/'
FILENAME = 'lac_pourvoi.csv'
OUTPUT_FILENAME = 'dataset_pourvoi_train.csv'

def main():

    df = load_csv_from_lac_to_df(PATH, FILENAME)
    print(df.shape)
    # Apply preprocessing to input features
    start = time()
    pipeline = Pipeline([
        ('apply_preprocessing', Preprocess()),
    ])
    df = pipeline.fit_transform(df)

    dump_df_to_csv(df, PATH, OUTPUT_FILENAME)
    print(f'Training dataset done in {round((time() - start) / 60)} min.')

if __name__ == '__main__':
    main()
