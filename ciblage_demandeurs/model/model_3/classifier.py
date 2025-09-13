# -*- coding: utf-8 -*-

"""
Random Forest Model
"""

import pandas as pd
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (
    roc_auc_score, accuracy_score, confusion_matrix, classification_report)

from utils.pipeline_classes import (DataFrameSelectColumns,
                                    DataFrameConvertNumeric,
                                    DataFrameConvertOneHotEncoding)

from model_1.model_functions import (
    filter_train_valid_sets_on_homogeneous_categ_features,
    build_X_y_arrays,
    preprocess,
    train_random_forest_classifier,
    train_logistic_regression_classifier,
    predict
)

TARGET_FEATURE = 'label'

CATEGORICAL_FEAT_LIST = [
    'dc_motifinscription_id',
    'previous_typepec',
    'previous_soustypepec',
    'previous_categoriede',
    'previous_motifinscription',
    'qualif',
    'nivfor',
    'dc_typepec_id',
    'dc_soustypepec',
    'dc_categoriede_id',
    #'rome_profil',
    #'last_typeperiodegaec',
    'dc_axetravail_id',
    # 'dc_axetravailprincipal_id',
]

NUMERICAL_FEAT_LIST = [
    'delta_dateinscriptionpec',
    'delta_previous_dateinscriptionpec',
    'h_trav_m',
    's_trav_m',
    'three_months_h_trav',
    'three_months_s_trav',
    'six_months_h_trav',
    'six_months_s_trav',
    'delta_last_periodeact',
    'periodeactgaec_total_count',
    'last_quantiteactivite',
    'pcs_counter',
    'naf_counter',
    'typepec_count',
    'ale_count',
    'numpec_count',
    'rome_profil_count',
    'montant_indem',
    'duree_indem',
    'delta_fin_indem',
    'ouverturedroit_count',
    'motif_fin_interim',
    #'six_months_maladie_sum',
    'ami_entrants_count',
    #'six_months_evenmtperso_sum',
    'typeperiodegaec_total_count',
    'has_rome',
    'has_rome_proche',
    'six_months_quantiteact_sum',
    'forma_count',
    #'typeforma_count',
    #'domforma_count',
    'comp_0',
    'comp_1',
    'comp_2',
    'comp_3',
    'comp_4',
    'comp_5',
    # 'comp_6',
    # 'comp_7',
    # 'comp_8',
    # 'comp_9',
    # 'comp_10',
    # 'comp_11',
    # 'comp_12',
    # 'comp_13',
    # 'comp_14',
]

def main(train_df, valid_df, code_rome):
    train_df[TARGET_FEATURE] = train_df[TARGET_FEATURE].astype(int)
    label_percent = len(train_df[train_df[TARGET_FEATURE] == 1]) / len(train_df)
    print(round(label_percent, 2))
    train_model_classifier = train_logistic_regression_classifier
    # Resample train set
    train_df1 = train_df[train_df.label == 1]
    train_df0 = train_df[train_df.label == 0]
    if label_percent >= 0.2:
        train_df = train_df0.iloc[::2].append(train_df1).sample(frac=1)
    else:
        train_df = train_df0.iloc[::3].append(train_df1).sample(frac=1)
    print(len(train_df), len(valid_df))

    train_df, valid_df = filter_train_valid_sets_on_homogeneous_categ_features(
        CATEGORICAL_FEAT_LIST, train_df, valid_df)

    X_train, y_train = build_X_y_arrays(train_df, TARGET_FEATURE,
                                        CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST)
    X_valid= build_X_y_arrays(valid_df, TARGET_FEATURE, CATEGORICAL_FEAT_LIST,
                              NUMERICAL_FEAT_LIST)
    # Preprocess features
    num_pipeline = Pipeline([
        ('col_selector', DataFrameSelectColumns(NUMERICAL_FEAT_LIST)),
        ('num_encoder', DataFrameConvertNumeric()),
        ('scaler', StandardScaler())
        ])
    cat_pipeline = Pipeline([
        ('col_selector', DataFrameSelectColumns(CATEGORICAL_FEAT_LIST)),
        ('le_encoder', DataFrameConvertOneHotEncoding())
        ])
    full_pipeline = FeatureUnion(transformer_list=[
        ('cat_pipeline', cat_pipeline),
        ('num_pipeline', num_pipeline)
        ])
    X_train, X_valid = preprocess(X_train, X_valid, full_pipeline)
    # Train model
    regr_fit = train_model_classifier(X_train, y_train)
    y_pred = predict(regr_fit, X_valid)
    predict_proba = regr_fit.predict_proba(X_valid)[:, 1]
    valid_df['pred'] = y_pred
    valid_df['predict_proba'] = predict_proba
    profils_df = valid_df.copy()
    rome_ref = pd.read_csv('./data/rome_ref.csv', sep='|')
    naf_ref = pd.read_csv('./data/naf_ref.csv', sep='|', encoding='latin')
    pcs_ref = pd.read_csv('./data/pcs_ref.csv', sep='|', encoding='latin')
    rome_ref.columns = ['rome', 'lbl_rome']
    naf_ref.columns = ['naf', 'lbl_naf_lieutrav']
    pcs_ref.columns = ['pcs', 'lbl_pcs']
    profils_df = profils_df.merge(
        rome_ref, left_on='rome_profil', right_on='rome').merge(
            naf_ref, left_on='last_naf_lieutrav', right_on='naf').merge(
                pcs_ref, left_on='last_pcs', right_on='pcs')
    naf_ref.columns = ['naf', 'lbl_naf_affect']
    profils_df = profils_df.merge(naf_ref, left_on='last_naf_affect', right_on='naf')
    comp_ref = pd.read_csv('./data/comp_noeud_ref.csv', sep='|', dtype='str')
    for i in range(3):
        comp_ref.columns = ['comp_profil', 'lbl_comp_profil_' + str(i)]
        profils_df = profils_df.merge(comp_ref, left_on='comp_profil_' + str(i), right_on='comp_profil')
    col_to_keep = ['ale', 'predict_proba', 'kn_individu_national', 'nom', 'prenom',
                   'age', 'sexe', 'datins', 'rome_profil', 'last_pcs', 'last_naf_affect',
                   'last_naf_lieutrav', 'lbl_rome', 'lbl_pcs', 'lbl_naf_lieutrav',
                   'lbl_naf_affect', 'etab_dep', 'residence_dep', 'last_dipl',
                   'lbl_comp_profil_0', 'lbl_comp_profil_1', 'lbl_comp_profil_2',
                   'is_interim', 'pred']
    profils_df = profils_df.sort_values(by='predict_proba')[col_to_keep]
    profils_df['rome'] = code_rome

    return profils_df
