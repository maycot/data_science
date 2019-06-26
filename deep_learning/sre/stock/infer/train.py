# -*- coding: utf-8 -*-

# Launch Tensorboard :
# tensorboard --logdir=runs:logs/ --reload_interval 30

import sys
import os
import logging
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

# Directories and filename parameters
data_dir = './data/'

data_filename = 'dataset_train.csv'
log_dir = 'logs'
save_dir = 'checkpoints'
model_dir = 'supervision'

# Network parameters

emb_list = [
'month',
'nreg',
'dep',
'bassin',
'domaine_pro',
'rome',
'motins',
'type_pop2',
'exper_classe',
'form',
'six_months_h_trav_categ',
'age_categ',
'qual',
'salaire_categ',
'montant_indem_q'
]

no_emb_list = [
'six_months_s_trav_categ_n',
'salaire_n',
'dpae_counter_n',
'delta3_pec_days_count_n',
'cat_reg_n',
'age_n',
'contrat_n',
'duree_indem_n',
'six_months_contact_ratio_categ_n',
'last_dpae_type_n',
'delta3_pec_count_n',
'three_months_contact_categ_n',
'delta1_pec_count_n',
'delta1_pec_days_count_n',
'temps_plein_n',
'dipl_n',
'mobil_n',
'has_ict1_n',
'axetrav_n',
'benefrsa_n',
'isentrep_n',
'th_n',
'mail_n',
'sms_n',
'matrimon_n',
'nenf_n',
'montant_indem_j_n',
'three_months_h_trav_categ_n',
'three_months_s_trav_categ_n',
'type1_delta1_pec_days_count_n',
'type2_delta1_pec_days_count_n',
'type3_delta1_pec_days_count_n',
'type4_delta1_pec_days_count_n',
'delta2_pec_count_n',
'type1_delta3_pec_days_count_n',
'type2_delta3_pec_days_count_n',
'type3_delta3_pec_days_count_n',
'type4_delta3_pec_days_count_n',
'score_forma_diag_n'
]

train_batch_size = 1500
step_size = 500
valid_batch_size = 1500
save = True

emb_size = 20

ema_decay = 0.99
bn_epsilon = 0.01

weight_initializer = tf.contrib.layers.xavier_initializer()

def build_graph(learning_rate, decay, train_df):

    model = DNN_model_with_embedding(name, train_df, log_dir,
                                     save_dir, emb_list, no_emb_list,
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

def train_model(restore, learning_rate, decay, train_df, n_epochs, save_model):
    """Train model for n_epochs.
    """
    model = build_graph(learning_rate, decay, train_df)

    logging.info('Train set shape : {}'.format(model.train_df.shape))
    logging.info("\nNumber of features in :\nembedded part: %d" %
          model.emb_layer.shape[1])
    logging.info("no embedded part : %d" % (len(model.no_emb_list)))
    logging.info("dnn input tensor: %d" % model.layers[0].shape[1])
    logging.info("\nNumber of trainable parameters = %d" %
          dnn_utils.num_parameters(model))
    n_batches = int(len(model.train_df) / train_batch_size)
    logging.info("\nBatches nb: %d\n" % n_batches)

    model.initialize_session(restore=restore)

    for i in range(n_epochs):
        logging.info("\n**** Epoch: %d ***\n" % i)
        start = time()
        model.train(n_batches, step_size, train_batch_size)
        if save_model:
            model.save_model()
        logging.info("\nEpoch done in {} min.".format(round((time() - start) / 60)))

    model.close_session()


def main():

    # Set the logger
    dnn_utils.set_logger(model_dir)
    # Load data
    start = time()
    train_df = dataset_utils.load_csv_to_df(data_dir, data_filename)
    logging.info("Done : load data in {} min.".format(round((time() - start)
                                                     / 60)))

    train_df = train_df[['ict1'] + emb_list + no_emb_list].dropna()
    # Run training for n epochs with different learning rates
    start = time()
    # epochs with LR=0.001
    train_model(True, 0.001, False, train_df, 1, False)
    # epochs with LR=0.0001
    train_model(True, 0.0001, False, train_df, 3, False)
    train_model(True, 0.0001, False, train_df, 3, False)
    # epochs with LR=0.00001
    train_model(True, 0.00001, False, train_df, 1, False)
    # Save model
    train_model(True, 0.00001, False, train_df, 1, True)

    logging.info("Training done in {} hours.".format(round((time() - start) / 3600)))


if __name__=='__main__':
    main()
