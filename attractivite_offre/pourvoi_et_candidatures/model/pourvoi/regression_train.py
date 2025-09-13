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
MODEL_PATH = os.path.join('pourvoi_ckpt', MODEL_NAME)

# Features params
TARGET_FEATURE = 'ln_delai_vie'


CATEGORICAL_FEAT_LIST = [
    #'dc_rome_id',
    #'naf',
    'dc_appelationrome_id',
    'dep',
    #'dc_communelieutravail',
    #'dom_pro',
    'naf3',
]

NUMERICAL_FEAT_LIST = [
    'delta_day',
    'is_interim',
    'is_cdi',
    'dc_qualification_id',
    'dc_topalertqltoffpremverif',
    'dc_topalertqltoffextraction',
    'dc_topdesquepossible',
    #'dc_typexperienceprof_id',
    'dc_typecontrat_id',
    'dc_typeaffichage_id',
    #'dc_unitedureecontrat',
    'dc_categorie_contrat',
    #'dc_categorie_experience',
    'dc_categorie_dureetravailhebdoheures',
    'dc_topinternet',
    'nivfor',
    'dc_typesalaire',
]

# Model params
MODEL_PARAMS = {
    'train_valid_split': 0.8,
    'epochs': 20,
    'batch_size': 550,
    'emb_config': {
        'dc_rome_id': 6,
        'naf': 6,
        'dc_appelationrome_id': 6,
        'dep': 4,
        'dc_communelieutravail': 6,
        'dom_pro': 6,
        'naf3': 6,
    },
}

def main(train_df, test_df, supervision_functions, pipeline_classes,
         regression_functions):

    # Set the logger
    checkpoint_file = \
        supervision_functions.create_logs_and_checkpoints_folders(MODEL_PATH, RESTORE)
    # train / valid sets
    train_df, test_df = \
        regression_functions.filter_train_valid_sets_on_homogeneous_categ_features(
            CATEGORICAL_FEAT_LIST, train_df, test_df)

    X_train, y_train = regression_functions.build_X_y_df(
        train_df, TARGET_FEATURE, CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST)
    X_valid = regression_functions.build_X_y_df(
        test_df, TARGET_FEATURE, CATEGORICAL_FEAT_LIST, NUMERICAL_FEAT_LIST)

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
    learning_rate = regression_functions.pourvoi_lr_schedule(MODEL_PARAMS['epochs'])
    model = regression_functions.pourvoi_build_model(
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
    y_pred = np.exp(y_pred).round()
    test_df['pred'] = y_pred
    test_df.delta_day = test_df.delta_day.astype(float)
    test_df.at[test_df.pred < test_df.delta_day, 'pred'] = \
        test_df.loc[test_df.pred < test_df.delta_day, 'delta_day'] + 1

    return test_df[['kc_offre', 'pred']]

if __name__ == '__main__':
    main()
