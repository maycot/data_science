# -*- coding: utf-8 -*-

"""
Train model
"""

# Launch Tensorboard :
# tensorboard --logdir= /

import logging
import os
import numpy as np
import pandas as pd
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import (roc_auc_score, accuracy_score, confusion_matrix,
                             classification_report)
from utils.supervision_functions import (create_logs_and_checkpoints_folders,
                                         set_logger)


from utils.pipeline_classes import (DataFrameSelectColumns,
                                    DataFrameConvertNumeric,
                                    DataFrameConvertLabelEncoding)

from model_2.model_functions import (
    filter_train_valid_sets_on_homogeneous_categ_features,
    build_X_y_df,
    preprocess,
    build_model,
    feed_network,
    lr_schedule,
    train_model,
    predict)

MODEL_NAME = 'dnn2'
RESTORE = False
MODEL_PATH = os.path.join('logs', MODEL_NAME)

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
    'rome_profil',
    'last_typeperiodegaec',
    'dc_axetravail_id',
    'dc_axetravailprincipal_id',
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

# Model params
MODEL_PARAMS = {
    'num_classes': 2,
    'restore': False,
    'epochs': 130,
    'batch_size': 350,
    'emb_config': {
        'dc_motifinscription_id': 3,
        'previous_typepec': 2,
        'previous_soustypepec': 2,
        'previous_categoriede': 3,
        'previous_motifinscription': 3,
        'qualif': 3,
        'nivfor': 3,
        'dc_typepec_id': 2,
        'dc_soustypepec': 2,
        'dc_categoriede_id': 3,
        'rome_profil': 8,
        'last_typeperiodegaec': 3,
        'dc_axetravail_id': 3,
        'dc_axetravailprincipal_id': 3,
    }
}

def main(train_df, valid_df, code_rome):

    model_dir, logs_dir, checkpoint_file = create_logs_and_checkpoints_folders(MODEL_PATH)

    train_df[TARGET_FEATURE] = train_df[TARGET_FEATURE].astype(int)
    label_percent = len(train_df[train_df[TARGET_FEATURE] == 1]) / len(train_df)
    print(round(label_percent, 2))
    # Resample train set
    train_df1 = train_df[train_df.label == 1]
    train_df0 = train_df[train_df.label == 0]
    train_df = train_df0.iloc[::3].append(train_df1).sample(frac=1)
    print(len(train_df), len(valid_df))

    train_df, valid_df = filter_train_valid_sets_on_homogeneous_categ_features(
        CATEGORICAL_FEAT_LIST, train_df, valid_df)

    X_train, y_train = build_X_y_df(train_df, TARGET_FEATURE,
                                        CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST)
    X_valid, y_valid = build_X_y_df(valid_df, TARGET_FEATURE,
                                        CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST)

    # Preprocess features
    num_pipeline = Pipeline([
        ('col_selector', DataFrameSelectColumns(NUMERICAL_FEAT_LIST)),
        ('num_encoder', DataFrameConvertNumeric()),
        ('scaler', StandardScaler())
    ])
    cat_pipeline = Pipeline([
        ('col_selector', DataFrameSelectColumns(CATEGORICAL_FEAT_LIST)),
        ('le_encoder', DataFrameConvertLabelEncoding())
    ])
    train_emb_array, train_no_emb_array, valid_emb_array, valid_no_emb_array = \
        preprocess(X_train, X_valid, cat_pipeline, num_pipeline)

    # Train model
    # get embedding max size
    train_emb_size = train_emb_array.max().to_dict()
    valid_emb_size = valid_emb_array.max().to_dict()
    emb_size = {key: max(value, valid_emb_size[key]) for
                key, value in train_emb_size.items()}
    
    learning_rate = lr_schedule(MODEL_PARAMS['epochs'])
    model = build_model(
        MODEL_PARAMS['restore'], checkpoint_file, MODEL_PARAMS['num_classes'],
        CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST, emb_size,
        MODEL_PARAMS['emb_config'], MODEL_PARAMS['epochs'], learning_rate)
    #print(model.summary())

    train_input_dict = feed_network(train_emb_array, train_no_emb_array)
    valid_input_dict = feed_network(valid_emb_array, valid_no_emb_array)

    history = train_model(
        model, train_input_dict, y_train, MODEL_PARAMS['epochs'],
        MODEL_PARAMS['batch_size'], checkpoint_file, logs_dir,
        validation_data=(valid_input_dict, y_valid))
    #print(history)

    # Log metrics
    metrics_dict = history.history
    for key, value in metrics_dict.items():
        metrics_dict[key] = [round(el, 2) for el in value]
    #print(metrics_dict)

    # Eval on y_valid with best model
    y_pred = predict(checkpoint_file, valid_input_dict)
    predict_proba = np.round(y_pred, 2).flatten()
    y_pred = np.round(predict_proba)
    auc = round(roc_auc_score(y_valid, predict_proba), 2)
    acc_score = round(accuracy_score(y_valid, y_pred), 2)
    print("auc : {}\naccuracy : {}".format(auc, acc_score))
    cm = confusion_matrix(y_valid, y_pred)
    print(cm)
    print(classification_report(y_valid, y_pred))
    # Results dataset
    # Results dataset
    valid_df['pred'] = y_pred
    valid_df['predict_proba'] = predict_proba
    profils_df = valid_df[valid_df.pred == 1]
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
                   'is_interim']
    profils_df = profils_df.sort_values(by='predict_proba')[col_to_keep]
    profils_df['rome'] = code_rome
    return profils_df


if __name__ == '__main__':
    main()
