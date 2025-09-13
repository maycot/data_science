# -*- coding: utf-8 -*-

"""
Fonction principale pour lancer le model "attractivité candidatures"

    1. Entrainement du model "Prédiction du nombre de candidatures de l'offre" :
       regression_train.py -> Prédit le nombre de candidatures de chaque offre.

"""
from datetime import date
import numpy as np
import pandas as pd
import utils.dataset_functions as dataset_functions
import utils.bucket_functions as bucket_functions
import utils.supervision_functions as supervision_functions
import utils.pipeline_classes as pipeline_classes
import model_functions.regression_functions as regression_functions

import pourvoi.regression_train as regression_train

DATA_DIR = './data/'
TRAIN_FILENAME_2019 = 'dataset_pourvoi_train_2019.csv'
TRAIN_FILENAME_2020 = 'dataset_pourvoi_train_2020.csv'
TEST_FILENAME = 'dataset_pourvoi_predict.csv'

def main():

    train_df1 = dataset_functions.load_csv_to_df(DATA_DIR, TRAIN_FILENAME_2019)
    train_df2 = dataset_functions.load_csv_to_df(DATA_DIR, TRAIN_FILENAME_2020)
    train_df = train_df1.append(train_df2)
    test_df = dataset_functions.load_csv_to_df(DATA_DIR, TEST_FILENAME)

    train_df['delai_vie'] = train_df['delai_vie'].astype(float)
    train_df1['delai_vie'] = train_df1['delai_vie'].astype(float)
    train_df1['ln_delai_vie'] = np.log(train_df1['delai_vie'])
    train_df1 = train_df1[~np.isinf(train_df1['delai_vie'])]
    print(f'train set size: {train_df1.shape} -- test set size: {test_df.shape}\n')

    print(f'offres pred start')
    offre_pred = pd.DataFrame()
    for _ in range(3):
        offre_df = regression_train.main(train_df1, test_df, supervision_functions,
                                         pipeline_classes, regression_functions)
        offre_pred = offre_pred.append(offre_df)
    offre_pred = offre_pred.groupby('kc_offre').mean().round()
    test_df = test_df.set_index('kc_offre').join(offre_pred)
    test_df = test_df[~test_df['pred'].isnull()]
    # On calcule les quantiles sur le train
    rome_dep_inf_15_mask = train_df.groupby(['dep', 'dc_rome_id']).kc_offre.count() < 15
    rome_dep_inf_15 = rome_dep_inf_15_mask[rome_dep_inf_15_mask].index.tolist()
    offre_not_to_pred = []
    offre_list = test_df.index.tolist()
    for kc_offre in offre_list:
        if (test_df.loc[kc_offre, 'dep'], test_df.loc[kc_offre, 'dc_rome_id']) in rome_dep_inf_15:
            offre_not_to_pred.append(kc_offre)
    test_df = test_df.reset_index()

    q33_dep_rome = train_df.groupby(
        ['dep', 'dc_rome_id']).delai_vie.quantile(0.33).round().reset_index()
    q33_dep_rome.columns = ['dep', 'dc_rome_id', 'q33_rome_dep']
    q66_dep_rome = train_df.groupby(
        ['dep', 'dc_rome_id']).delai_vie.quantile(0.66).round().reset_index()
    q66_dep_rome.columns = ['dep', 'dc_rome_id', 'q66_rome_dep']
    q_dep_rome = q33_dep_rome.merge(q66_dep_rome, on=['dep', 'dc_rome_id'])
    q33_rome = train_df.groupby(
        ['dc_rome_id']).delai_vie.quantile(0.33).round().reset_index()
    q33_rome.columns = ['dc_rome_id', 'q33_rome']
    q66_rome = train_df.groupby(
        ['dc_rome_id']).delai_vie.quantile(0.66).round().reset_index()
    q66_rome.columns = ['dc_rome_id', 'q66_rome']
    q_rome = q33_rome.merge(q66_rome, on=['dc_rome_id'])
    # quantile_df = q_dep_rome.merge(q_rome, on='dc_rome_id')
    # quantile_df = pd.read_csv('./data/quantile.csv', sep='|')
    test_df = test_df.merge(q_dep_rome, on=['dep', 'dc_rome_id']).merge(
        q_rome, on=['dc_rome_id'])
    test_df['q33_contxt'] = 0
    test_df.at[test_df.kc_offre.isin(offre_not_to_pred), 'q33_contxt'] = \
        test_df.loc[test_df.kc_offre.isin(offre_not_to_pred), 'q33_rome']
    test_df.at[~test_df.kc_offre.isin(offre_not_to_pred), 'q33_contxt'] = \
        test_df.loc[~test_df.kc_offre.isin(offre_not_to_pred), 'q33_rome_dep']
    test_df['q66_contxt'] = 0
    test_df.at[test_df.kc_offre.isin(offre_not_to_pred), 'q66_contxt'] = \
        test_df.loc[test_df.kc_offre.isin(offre_not_to_pred), 'q66_rome']
    test_df.at[~test_df.kc_offre.isin(offre_not_to_pred), 'q66_contxt'] = \
        test_df.loc[~test_df.kc_offre.isin(offre_not_to_pred), 'q66_rome_dep']
    test_df['q33_contxt'] = test_df['q33_contxt'] + 2
    # test_df['q66_contxt'] = test_df['q66_contxt'] - 2
    # On ajoute la classe d'attractivité
    test_df['pred_attract_class'] = 2
    test_df.at[test_df['pred'] <= test_df['q33_contxt'], 'pred_attract_class'] = 1
    test_df.at[test_df['pred'] >= test_df['q66_contxt'], 'pred_attract_class'] = 3

    # On ajoute les indices de confiances pour la prédiction
    test_df['ic'] = 4
    test_df.at[test_df['pred'] > 35, 'ic'] = 8
    test_df['pred-ic'] = (test_df.pred - test_df.ic).round()
    test_df['pred+ic'] = (test_df.pred + test_df.ic).round()
    test_df.at[test_df['pred-ic'] < 0, 'pred-ic'] = 0

    test_df = test_df[['kc_offre', 'pred_attract_class', 'pred', 'pred-ic',
                       'pred+ic', 'q33_contxt', 'q66_contxt', 'dc_rome_id', 'dep']]

    feat_to_round = ['pred', 'q33_contxt', 'q66_contxt']
    test_df[feat_to_round] = test_df[feat_to_round].round()

    dataset_functions.dump_df_to_csv(test_df, DATA_DIR, 'pourvoi.csv')
    """
    day = date.today().strftime("%Y-%m-%d")
    bucket_functions.send_file(
        './data/pourvoi.csv', 'prediction-offre/cadre-r/pourvoi.csv')
    bucket_functions.send_file(
        './data/pourvoi.csv', 'prediction-offre/cadre-p/pourvoi.csv')
    bucket_functions.send_file(
        './data/pourvoi.csv', 'predictions_sauvegarde/pourvoi_' + day + '.csv')
    """

if __name__ == '__main__':
    main()
