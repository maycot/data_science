# -*- coding: utf-8 -*-

"""
Utility functions for MLP model
"""

import os
import logging
from typing import Dict, List
import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from sklearn.metrics import confusion_matrix
import tensorflow as tf
from tensorflow.keras.utils import to_categorical


def filter_train_valid_sets_on_homogeneous_categ_features(categorical_feat_list:
    List[str], train_df: pd.DataFrame, valid_df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows in train and/or valid sets with uncommon categorical features.
    """

    logging.info(f'Before filtering : train set shape: {train_df.shape}\
        / valid set shape: {valid_df.shape}')
    for categ in categorical_feat_list:
        union_list = list(set(train_df[categ]).intersection(valid_df[categ]))
        train_df = train_df[train_df[categ].isin(union_list)]
        valid_df = valid_df[valid_df[categ].isin(union_list)]
    logging.info(f'After filtering : train set shape: {train_df.shape}\
        / valid set shape: {valid_df.shape}')

    return train_df, valid_df

def build_X_y_df(df: pd.DataFrame, target_name: str, *lists) -> pd.DataFrame:
    """Build X and y arrays"""
    X = df[sum(lists, [])]
    if target_name in df.columns:
        y = df[target_name].values
        return X, y

    return X

def preprocess(X_train: pd.DataFrame, X_valid: pd.DataFrame,
               cat_pipeline, num_pipeline) -> pd.DataFrame:
    """Apply preprocessing on X arrays: embeddings / no embeddings arrays """

    # Numerical features not to embed
    train_no_emb = num_pipeline.fit_transform(X_train)
    valid_no_emb = num_pipeline.transform(X_valid)
    # Categorical features to embed
    train_emb = cat_pipeline.fit_transform(X_train)
    valid_emb = cat_pipeline.transform(X_valid)

    return train_emb, train_no_emb, valid_emb, valid_no_emb

def build_model(restore: bool, checkpoint_file: str, num_classes: int,
                categorical_feat_list: List[str], numerical_feat_list: List[str],
                emb_size: Dict[str, int], emb_config: Dict[str, int],
                epochs: int, lr: int):
    """MLP models : 3 dense layers with embeddings for categorical features """
    if restore and os.path.isfile(checkpoint_file):
        model = tf.keras.models.load_model(checkpoint_file)
    else:
        model_layers = []
        input_layers = []
        # Embedding part for categorical features
        emb_initializer = tf.keras.initializers.lecun_uniform(seed=None)
        for feat in categorical_feat_list:
            inpt = tf.keras.layers.Input(shape=(1,), name=feat)
            input_layers.append(inpt)
            embed = tf.keras.layers.Embedding(emb_size[feat] + 1,
                                              emb_config[feat],
                                              trainable=True,
                                              embeddings_initializer=emb_initializer
                                              )(inpt)
            embed_reshaped = tf.keras.layers.Reshape(
                target_shape=(emb_config[feat],))(embed)
            model_layers.append(embed_reshaped)
        if numerical_feat_list:
            # Numerical features part
            num_input = tf.keras.layers.Input(
                shape=(len(numerical_feat_list)), name='num_features')
            input_layers.append(num_input)
            model_layers.append(num_input)

        # Concat all model layers
        merge_models = tf.keras.layers.concatenate(model_layers)
        # Dense layers on all vectors
        # Dense layer params
        activation = 'elu'
        kernel_initializer = 'lecun_uniform'
        bias_initializer1 = tf.keras.initializers.Constant(0)
        bias_initializer2 = tf.keras.initializers.Constant(0)
        bias_initializer3 = tf.keras.initializers.Constant(0)
        use_bias = True
        #kernel_regularizer = tf.keras.regularizers.l1(l=0.0)

        pre_preds = tf.keras.layers.Dense(
            60, activation=activation, kernel_initializer=kernel_initializer,
            use_bias=use_bias, bias_initializer=bias_initializer1)(merge_models)
        pre_preds = tf.keras.layers.BatchNormalization()(pre_preds)
        pre_preds = tf.keras.layers.Dense(
            20, activation=activation, kernel_initializer=kernel_initializer,
            use_bias=use_bias, bias_initializer=bias_initializer2)(pre_preds)
        pre_preds = tf.keras.layers.BatchNormalization()(pre_preds)
        pre_preds = tf.keras.layers.Dense(
            5, activation=activation, kernel_initializer=kernel_initializer,
            use_bias=use_bias, bias_initializer=bias_initializer3)(pre_preds)
        pre_preds = tf.keras.layers.BatchNormalization()(pre_preds)
        pred = tf.keras.layers.Dense(1, activation='sigmoid')(pre_preds)
        model = tf.keras.models.Model(inputs=input_layers, outputs=pred)

        # training params
        loss = 'binary_crossentropy'
        metrics = [
            tf.keras.metrics.CategoricalAccuracy(name='accuracy'),
            tf.keras.metrics.Precision(name='precision'),
            tf.keras.metrics.Recall(name='recall'),
        ]
        optimizer = tf.keras.optimizers.Adam(lr)
        model.compile(loss=loss, metrics=metrics, optimizer=optimizer)

    return model

def feed_network(emb_df: pd.DataFrame, no_emb_df: pd.DataFrame):
    """Return dict with inputs layers to feed model """

    input_dict = {}
    input_dict['num_features'] = no_emb_df
    for input_name in emb_df.columns:
        input_dict[input_name] = emb_df[input_name].values

    return input_dict

def lr_schedule(epoch):
    """ Returns a custom learning rate that decreases as epochs progress """
    learning_rate = 0.1
    if epoch > 5:
        learning_rate = 0.01
    if epoch > 50:
        learning_rate = 0.0001

    tf.summary.scalar('learning rate', data=learning_rate, step=epoch)
    return learning_rate

def train_model(model, train_input_dict: Dict[str, np.ndarray],
                y_train: np.ndarray, epochs: int, batch_size: int, 
                checkpoint_file: str, log_dir: str, validation_data=None):
    """Train MLP model."""

    if validation_data:
        # early_stopping = tf.keras.callbacks.EarlyStopping(
        #     monitor='val_accuracy',
        #     verbose=1,
        #     patience=10,
        #     mode='max',
        #     restore_best_weights=True
        # )
        model_checkpoint = tf.keras.callbacks.ModelCheckpoint(
            checkpoint_file,
            save_best_only=True
        )
        tensorboard_logs = tf.keras.callbacks.TensorBoard(log_dir=log_dir)
        history = model.fit(
            train_input_dict,
            y_train,
            validation_data=validation_data,
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[model_checkpoint, tensorboard_logs],
            verbose=0
        )
    else:
        model_checkpoint = tf.keras.callbacks.ModelCheckpoint(checkpoint_file)
        history = model.fit(
            train_input_dict,
            y_train,
            epochs=epochs,
            batch_size=batch_size,
            callbacks=[model_checkpoint],
            verbose=0
        )

    return history

def predict(checkpoint_file: str, valid_input_dict) -> np.ndarray:
    """Make predictions from registered model """

    model = tf.keras.models.load_model(checkpoint_file)
    y_pred = model.predict(valid_input_dict)

    return y_pred

def plot_metrics(history, model_dir):
    """Plot training metrics on train and valid set."""

    metrics =  ['loss', 'accuracy', 'precision', 'recall']
    plt.figure(figsize=(10, 12))
    for n, metric in enumerate(metrics):
        name = metric.replace("_"," ").capitalize()        
        plt.subplot(4, 1, n+1)
        plt.plot(history.epoch,  history.history[metric], color='b',
                 label='Train')
        plt.plot(history.epoch, history.history['val_'+ metric],
                 color='r', label='Val')
        plt.xlabel('Epoch')
        plt.ylabel(name)
        plt.tight_layout()
    plt.legend()
    plt.savefig(os.path.join(model_dir, 'metrics_plot.png'))

def plot_confusion_matrix(y_true: np.ndarray, y_pred: np.ndarray,
                          model_dir: str):
    """Plot confusion matrix for valid set."""
    cm = confusion_matrix(y_true, y_pred)
    logging.info(cm)
    plt.figure(figsize=(10, 10))
    sns.heatmap(cm, annot=True, fmt="d", cmap="Blues")
    plt.title('Confusion matrix')
    plt.ylabel('Actual label')
    plt.xlabel('Predicted label')
    plt.savefig(os.path.join(model_dir, 'cm_plot.png'))
