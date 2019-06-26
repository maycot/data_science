# -*- coding: utf-8 -*-

# Launch Tensorboard :
# tensorboard --logdir=runs:logs/ --reload_interval 30

import sys
import os
import numpy as np
import pandas as pd
import tensorflow as tf
from time import time

from utils import dataset_utils, dnn_utils
from model import DNN_model_with_embedding

###
# The model name : define log and checkpoint directories
###
name = 'dnn1'

# Define train and valid set parameters
year_valid = 2017
month_valid_start = 1
month_valid_end = 3
two_years = False

# Directories and filename parameters
data_dir = './data/'
data_filename = 'dataset_dnn.csv'
log_dir = 'logs'
save_dir = 'checkpoints'

# Network parameters

emb_list = ['month', 'dep', 'bassin', 'cat_reg', 'motins', 'rome',
            'domaine_pro', 'npec', 'nreg', 'form', 'qual',
            'montant_indem_q', 'h_trav', 'age_categ']

no_emb_quanti_list = ['salaire', 'duree_indem', 'montant_indem_j', 's_trav',
                      'mobil', 'axetrav', 'benefrsa', 'exper_classe', 'th',
                      'temps_plein', 'nenf', 'resqpv', 'dipl', 'matrimon',
                      'sms', 'mail', 'isctp', 'isentrep',
                      'score_forma_diag', 'age']

no_emb_quali_list = ['contrat', 'sexe']

train_batch_size = 1500
step_size = 500
valid_batch_size = 1500
save = True

emb_size = 20

ema_decay = 0.99
bn_epsilon = 0.01

weight_initializer = tf.contrib.layers.xavier_initializer()


def build_graph(learning_rate, decay, train_df, valid_df):

    model = DNN_model_with_embedding(name, train_df, valid_df,
                                     log_dir, save_dir, emb_list,
                                     no_emb_quanti_list, no_emb_quali_list,
                                     emb_size, ema_decay, bn_epsilon)

    model.add_fc('l1', size=1550, weight_initializer=weight_initializer,
                 init_bias=0)
    model.add_batch_norm('bn_1')
    model.add_activation('elu')
    #model.add_dropout(0.5)
    model.add_fc('l2', size=330, weight_initializer=weight_initializer,
                 init_bias=0)
    model.add_batch_norm('bn_2')
    model.add_activation('elu')
    #model.add_dropout(0.5)
    model.add_fc('l3', size=10, weight_initializer=weight_initializer,
                 init_bias=0)
    model.add_batch_norm('bn_3')
    model.add_activation('elu')
    #model.add_dropout(0.5)
    model.add_fc('output', size=1, weight_initializer=weight_initializer,
                 init_bias=0)
    model.add_batch_norm('bn_4')
    model.add_activation('sigmoid')
    model.add_loss('loss')
    model.add_adam_optimizer('optimizer', init_learning_rate=learning_rate,
                             decay=decay)
    model.add_summaries('summaries')

    return model

def train_model(restore, learning_rate, decay, train_df, valid_df,
                n_epochs):
    """Train model for n_epochs.
    """
    model = build_graph(learning_rate, decay, train_df, valid_df)

    print('Train set shape : {}'.format(model.train_df.shape))
    print('Valid set shape : {}'.format(model.valid_df.shape))
    print("\nNumber of features in :\nembedded part: %d" %
          model.emb_layer.shape[1])
    print("no embedded part (quanti + quali): %d" %
          (len(model.no_emb_quanti_list) + len(model.no_emb_quali_list)))
    print("dnn input tensor: %d" % model.layers[0].shape[1])
    print("\nNumber of trainable parameters = %d" %
          dnn_utils.num_parameters(model))
    n_batches = int(len(model.train_df) / train_batch_size)
    print("\nBatches nb: %d\n" % n_batches)

    model.initialize_session(restore=restore)

    for i in range(n_epochs):
        print("\n-- Epoch: %d" % i)
        model.train(n_batches, step_size, train_batch_size, valid_batch_size,
                    save)

    model.close_session()



def main():

    # Load data
    start = time()
    train_df, valid_df = dataset_utils.make_train_valid_set2(
        data_dir, data_filename, year_valid, month_valid_start,
        month_valid_end, two_years)
    print("Done : load data in {} min.".format(round((time() - start)
                                                     / 60)))

    train_df = train_df[['ict1'] + emb_list + \
                        no_emb_quanti_list + no_emb_quali_list].dropna()
    valid_df = valid_df[['ict1'] + emb_list +  \
                        no_emb_quanti_list + no_emb_quali_list].dropna()

    for el in ["rome", "bassin"]:
        if el in emb_list:
            el_in_valid_not_train = set(valid_df[el].values
                                         ).symmetric_difference(
                                         set(train_df[el].values))
            if el_in_valid_not_train:
                print("{} in valid set but not in train set \
                : {}".format(el, el_in_valid_not_train))
                valid_df = valid_df[~valid_df[el].isin(el_in_valid_not_train)]

    # Run training for n epochs with different learning rates
    start = time()
    # epochs with LR=0.0001
    train_model(True, 0.0001, False, train_df, valid_df, 10)
    # epochs with LR=0.00001
    train_model(True, 0.00001, False, train_df, valid_df, 5)

    print("Training done in {} min.".format(round((time() - start) / 60)))

if __name__=='__main__':
    main()
