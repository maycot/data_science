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
from final_model import DNN_model_with_embedding

###
# The model name : define log and checkpoint directories
###
name = 'dnn1'

# Directories and filename parameters
data_dir = './data/'

train_filename = 'dataset_stock_train_7_9.csv'
valid_filename = 'dataset_stock_test_7_9.csv'
log_dir = 'logs'
save_dir = 'checkpoints'

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
'six_months_s_trav_categ',
'salaire',
'dpae_counter',
'delta3_pec_days_count',
'cat_reg',
'age',
'contrat',
'duree_indem',
'six_months_contact_ratio_categ',
'last_dpae_type',
'delta3_pec_count',
'three_months_contact_categ',
'delta1_pec_count',
'delta1_pec_days_count',
'temps_plein',
'dipl',
'mobil',
'has_ict1',
'axetrav',
'benefrsa',
'isentrep',
'th',
'mail',
'sms',
'matrimon',
'nenf',
'montant_indem_j',
'three_months_h_trav_categ',
'three_months_s_trav_categ',
'type1_delta1_pec_days_count',
'type2_delta1_pec_days_count',
'type3_delta1_pec_days_count',
'type4_delta1_pec_days_count',
'delta2_pec_count',
'type1_delta3_pec_days_count',
'type2_delta3_pec_days_count',
'type3_delta3_pec_days_count',
'type4_delta3_pec_days_count',
'score_forma_diag'
]

train_batch_size = 1500
step_size = 500
valid_batch_size = 1500
save = True

max_emb_size = 20

ema_decay = 0.99
bn_epsilon = 0.01

weight_initializer = tf.contrib.layers.xavier_initializer()

def build_graph(learning_rate, decay, train_df, valid_df):

    model = DNN_model_with_embedding(name, train_df, valid_df,
                                     log_dir, save_dir, emb_list,
                                     no_emb_list, max_emb_size, ema_decay,
                                     bn_epsilon)

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
    print("no embedded part : %d" % (len(model.no_emb_list)))
    print("dnn input tensor: %d" % model.layers[0].shape[1])
    print("\nNumber of trainable parameters = %d" %
          dnn_utils.num_parameters(model))
    n_batches = int(len(model.train_df) / train_batch_size)
    print("\nBatches nb: %d\n" % n_batches)

    model.initialize_session(restore=restore)

    for i in range(n_epochs):
        print("\n**** Epoch: %d ***\n" % i)
        start = time()
        model.train(n_batches, step_size, train_batch_size, valid_batch_size,
                    save)
        print("\nEpoch done in {} min.".format(round((time() - start) / 60)))

    model.close_session()



def main():

    # Load data
    start = time()
    train_df = pd.read_csv(os.path.join(data_dir, train_filename))
    valid_df = pd.read_csv(os.path.join(data_dir, valid_filename))
    #valid_df = dataset_utils.load_csv_to_df(data_dir, valid_filename)
    print("Done : load data in {} min.".format(round((time() - start)
                                                     / 60)))

    #valid_df = valid_df[valid_df.has_ict1 == 1]
    valid_df = valid_df[valid_df.type_pop2 == 1]
    train_df = train_df[['ict1'] + emb_list + no_emb_list].dropna()
    valid_df = valid_df[['ict1'] + emb_list +  no_emb_list].dropna()
    train_df = train_df.reset_index()
    valid_df = valid_df.reset_index()
    # Run training for n epochs with different learning rates
    start = time()
    # epochs with LR=0.0001
    #train_model(True, 0.001, False, train_df, valid_df, 2)
    #train_model(True, 0.0001, False, train_df, valid_df, 6)
    #train_model(True, 0.00001, False, train_df, valid_df, 2)
    # epochs with LR=0.00001
    train_model(True, 0.00001, False, train_df, valid_df, 1)

    print("Training done in {} min.".format(round((time() - start) / 60)))

if __name__=='__main__':
    main()
