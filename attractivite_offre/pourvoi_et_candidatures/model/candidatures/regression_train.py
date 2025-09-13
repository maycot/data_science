# -*- coding: utf-8 -*-

"""
Train model
"""

# Launch Tensorboard :
# tensorboard --logdir=r_model/offres

import os
import numpy as np
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

# Directories and filename params
MODEL_NAME = 'dnn1'
RESTORE = False
MODEL_PATH = os.path.join('cdt_ckpt', MODEL_NAME)

# Features params
TARGET_FEATURE = 'ln_total_cdt'

CATEGORICAL_FEAT_LIST = [
    'dc_topinternet',
    'dc_modepreselection_id',
    'dc_typeaffichage_id',
    'dc_typecontrat_id',
    'dc_topalertqltoffpremverif',
    'dc_topalertqltoffextraction',
    'dc_acteur_miseajour1_id',
    'dc_topdesquepossible',
    'nivfor',
    'dc_typexperienceprof_id',
    'dc_categorie_dureetravailhebdoheures',
    'dc_qualification_id',
    'domaine_pro',
    'dc_typesalaire',
    'dn_statutetablissement',
    'dc_idcdifficulteeconomique_id',
    'dc_trancheeffectifetab',
    'dc_categorie_experience',
    'dc_categorie_contrat',
    'dep',
    'wd',
    'dc_rome_id',
    'naf',
    'naf3',
    'be19',
    'dc_appelationrome_id',
]

NUMERICAL_FEAT_LIST = [
    'ict1',
    'ict3',
    'ict10',
    'prin1',
    'prin2',
    'prin3',
    'DE_ABC_rome_be19',
    'commune_rech_counter',
    'is_commune_50',
    'is_dep_5',
    'is_titre',
    'is_cdi',
    'is_interim',
    'cdt_delta_day',
    'persp2',
    'delta_cdt_wd',
    'cdt_count'
]

# Model params
MODEL_PARAMS = {
    'epochs': 20,
    'batch_size': 550,
    'emb_config': {
        'dc_topinternet': 2,
        'dc_modepreselection_id': 2,
        'dc_typeaffichage_id': 3,
        'dc_topalertqltoffpremverif': 2,
        'dc_topalertqltoffextraction': 2,
        'dc_acteur_miseajour1_id': 2,
        'dc_topdesquepossible': 2,
        'dc_typecontrat_id': 13,
        'nivfor': 3,
        'dc_categorie_dureetravailhebdoheures': 3,
        'dc_qualification_id': 3,
        'domaine_pro': 20,
        'dc_typesalaire': 2,
        'dn_statutetablissement': 5,
        'dc_idcdifficulteeconomique_id': 3,
        'dc_trancheeffectifetab': 5,
        'dep': 10,
        'dc_rome_id': 20,
        'naf3': 15,
        'naf': 10,
        'be19': 8,
        'dc_communelieutravail': 8,
        'dc_typexperienceprof_id': 7,
        'dc_categorie_contrat': 3,
        'dc_categorie_experience': 8,
        'dc_appelationrome_id': 15,
        'wd': 5,
    },
}

# Model params
MODEL_PARAMS = {
    'epochs': 20,
    'batch_size': 550,
    'emb_config': {
        'dc_topinternet': 2,
        'dc_modepreselection_id': 2,
        'dc_typeaffichage_id': 3,
        'dc_topalertqltoffpremverif': 2,
        'dc_topalertqltoffextraction': 2,
        'dc_acteur_miseajour1_id': 2,
        'dc_topdesquepossible': 2,
        'dc_typecontrat_id': 13,
        'nivfor': 3,
        'dc_categorie_dureetravailhebdoheures': 3,
        'dc_qualification_id': 3,
        'domaine_pro': 20,
        'dc_typesalaire': 2,
        'dn_statutetablissement': 5,
        'dc_idcdifficulteeconomique_id': 3,
        'dc_trancheeffectifetab': 5,
        'dep': 10,
        'dc_rome_id': 20,
        'naf3': 15,
        'naf': 10,
        'be19': 8,
        'dc_communelieutravail': 8,
        'dc_typexperienceprof_id': 7,
        'dc_categorie_contrat': 3,
        'dc_categorie_experience': 8,
        'dc_appelationrome_id': 15,
        'wd': 5,
    },
}

def main(train_df, valid_df, supervision_functions, pipeline_classes,
         regression_functions):

    # Set the logger
    checkpoint_file = \
        supervision_functions.create_logs_and_checkpoints_folders(MODEL_PATH, RESTORE)
    # train / valid sets
    train_df, valid_df = \
        regression_functions.filter_train_valid_sets_on_homogeneous_categ_features(
            CATEGORICAL_FEAT_LIST, train_df, valid_df)

    X_train, y_train = regression_functions.build_X_y_df(
        train_df, TARGET_FEATURE, CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST)
    X_valid = regression_functions.build_X_y_df(
        valid_df, TARGET_FEATURE, CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST)

    # Preprocess features
    num_pipeline = Pipeline([
        ('col_selector', pipeline_classes.DataFrameSelectColumns(NUMERICAL_FEAT_LIST)),
        ('num_encoder', pipeline_classes.DataFrameConvertNumeric()),
        ('scaler', StandardScaler())
    ])
    cat_pipeline = Pipeline([
        ('col_selector', pipeline_classes.DataFrameSelectColumns(CATEGORICAL_FEAT_LIST)),
        ('le_encoder', pipeline_classes.DataFrameConvertLabelEncoding())
    ])
    train_emb_array, train_no_emb_array, valid_emb_array, valid_no_emb_array = \
        regression_functions.preprocess(X_train, X_valid, cat_pipeline, num_pipeline)
    # Train model
    # get embedding max size
    train_emb_size = train_emb_array.max().to_dict()
    valid_emb_size = valid_emb_array.max().to_dict()
    emb_size = {key: max(value, valid_emb_size[key]) for
                key, value in train_emb_size.items()}
    # build model
    learning_rate = regression_functions.cdt_lr_schedule(MODEL_PARAMS['epochs'])
    model = regression_functions.build_model(
        RESTORE, checkpoint_file, CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST,
        emb_size, MODEL_PARAMS['emb_config'], learning_rate)

    train_input_dict = regression_functions.feed_network(train_emb_array, train_no_emb_array)
    valid_input_dict = regression_functions.feed_network(valid_emb_array, valid_no_emb_array)

    history = regression_functions.train_model(
        model, train_input_dict, y_train, MODEL_PARAMS['epochs'],
        MODEL_PARAMS['batch_size'], checkpoint_file)

    # Eval on y_valid with best model
    y_pred = regression_functions.predict(checkpoint_file, valid_input_dict)
    y_pred = y_pred.flatten()
    y_pred = np.exp(y_pred)
    valid_df['pred'] = y_pred
    valid_df.cdt_count = valid_df.cdt_count.astype(float)
    valid_df.at[valid_df.pred > 500, 'pred'] = 500
    valid_df.at[valid_df.pred < valid_df.cdt_count, 'pred'] = \
        valid_df.loc[valid_df.pred < valid_df.cdt_count, 'pred']

    return valid_df[['kc_offre', 'pred']]
