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
import candidatures.regression_train as regression_train

DATA_DIR = './data/'
TRAIN_FILENAME = 'dataset_cdt_train.csv'
TEST_FILENAME = 'dataset_cdt_predict.csv'
TO_PRED_DAY = date.today()
#TO_PRED_DAY = date.today() - timedelta(15)

def main():

    train_df = dataset_functions.load_csv_to_df(DATA_DIR, TRAIN_FILENAME)
    train_df['total_cdt'] = train_df['total_cdt'].astype(float)
    train_df['ln_total_cdt'] = np.log(train_df['total_cdt'])
    train_df = train_df[~np.isinf(train_df['ln_total_cdt'])]
    test_df = dataset_functions.load_csv_to_df(DATA_DIR, TEST_FILENAME)
    """
    test_day_start = (TO_PRED_DAY - timedelta(days=14)).strftime("%Y-%m-%d")
    test_day_end = TO_PRED_DAY.strftime("%Y-%m-%d")
    test_df = df[(df.dd_datecreationreport >= test_day_start) &
                 (df.dd_datecreationreport < test_day_end)]
    """
    test_df.cdt_delta_day = test_df.cdt_delta_day.astype(float)
    test_df = test_df.sort_values(['kc_offre', 'cdt_delta_day']).groupby('kc_offre').last()
    test_df = test_df.reset_index()
    print(f'train set size: {train_df.shape} -- test set size: {test_df.shape}\n')

    print(f'offres pred start')
    offre_pred = pd.DataFrame()
    for _ in range(3):
        offre_df = regression_train.main(train_df, test_df, supervision_functions,
                                         pipeline_classes, regression_functions)
        offre_pred = offre_pred.append(offre_df)
    offre_pred = offre_pred.groupby('kc_offre').mean().round()

    test_df = test_df.set_index('kc_offre')
    test_df = test_df.join(offre_pred, how='inner')
    test_df = test_df[~test_df['pred'].isnull()]
    # On calcule les quantiles sur le train
    train_df = train_df.groupby('kc_offre').first().reset_index()
    rome_dep_inf_15_mask = train_df.groupby(
        ['dep', 'dc_rome_id']).kc_offre.count() < 15
    rome_dep_inf_15 = rome_dep_inf_15_mask[rome_dep_inf_15_mask].index.tolist()
    offre_not_to_pred = []
    offre_list = test_df.index.tolist()
    for kc_offre in offre_list:
        if (test_df.loc[kc_offre, 'dep'], test_df.loc[kc_offre, 'dc_rome_id']) in rome_dep_inf_15:
            offre_not_to_pred.append(kc_offre)
    test_df = test_df.reset_index()

    q33_dep_rome = train_df.groupby(
        ['dep', 'dc_rome_id']).total_cdt.quantile(0.33).round().reset_index()
    q33_dep_rome.columns = ['dep', 'dc_rome_id', 'q33_rome_dep']
    q66_dep_rome = train_df.groupby(
        ['dep', 'dc_rome_id']).total_cdt.quantile(0.66).round().reset_index()
    q66_dep_rome.columns = ['dep', 'dc_rome_id', 'q66_rome_dep']
    q_dep_rome = q33_dep_rome.merge(q66_dep_rome, on=['dep', 'dc_rome_id'])
    q33_rome = train_df.groupby(
        ['dc_rome_id']).total_cdt.quantile(0.33).round().reset_index()
    q33_rome.columns = ['dc_rome_id', 'q33_rome']
    q66_rome = train_df.groupby(
        ['dc_rome_id']).total_cdt.quantile(0.66).round().reset_index()
    q66_rome.columns = ['dc_rome_id', 'q66_rome']
    q_rome = q33_rome.merge(q66_rome, on=['dc_rome_id'])

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
    # On ajoute la classe d'attractivité
    test_df['pred_attract_class'] = 2
    test_df.at[test_df['pred'] <= test_df['q33_contxt'], 'pred_attract_class'] = 1
    test_df.at[test_df['pred'] >= test_df['q66_contxt'], 'pred_attract_class'] = 3

    # On ajoute les indices de confiances pour la prédiction
    test_df['ic'] = 2
    test_df.at[test_df['pred'] > 10, 'ic'] = 3
    test_df['pred-ic'] = (test_df.pred - test_df.ic).round()
    test_df['pred+ic'] = (test_df.pred + test_df.ic).round()
    test_df.at[test_df['pred-ic'] < 0, 'pred-ic'] = 0


    test_df = test_df[['kc_offre', 'pred_attract_class', 'pred', 'pred-ic',
                       'pred+ic', 'q33_contxt', 'q66_contxt', 'dc_rome_id', 'dep']]

    feat_to_round = ['pred', 'q33_contxt', 'q66_contxt']
    test_df[feat_to_round] = test_df[feat_to_round].round()

    day = date.today().strftime("%Y-%m-%d")

    pourvoi_df = dataset_functions.load_csv_to_df(DATA_DIR, 'pourvoi.csv')
    offre_inter = set(pourvoi_df.kc_offre).intersection(set(test_df.kc_offre))
    pourvoi_df = pourvoi_df[pourvoi_df.kc_offre.isin(offre_inter)]
    test_df = test_df[test_df.kc_offre.isin(offre_inter)]
    dataset_functions.dump_df_to_csv(pourvoi_df, DATA_DIR, 'pourvoi.csv')
    dataset_functions.dump_df_to_csv(test_df, DATA_DIR, 'candidature.csv')
    for filename in ['pourvoi', 'candidature']:
        bucket_functions.send_file(
            './data/' + filename + '.csv', 'prediction-offre/cadre-r/' + filename + '.csv')
        bucket_functions.send_file(
            './data/' + filename + '.csv', 'prediction-offre/cadre-p/' + filename + '.csv')
        bucket_functions.send_file(
            './data/' + filename + '.csv', 'predictions_sauvegarde/' + filename + '_' + day + '.csv')

if __name__ == '__main__':
    main()
