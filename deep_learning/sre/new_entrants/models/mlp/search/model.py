# -*- coding: utf-8 -*-

###
# DNN Model with entity embeddings - SRE V2
###


import sys
import os
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from tensorflow.python.ops.init_ops import UniformUnitScaling, Constant

from utils.pipeline_preprocess_class import (DataFrameSelectColumns,
                                             DataFrameConvertNumeric,
                                             DataFrameConvertLabelEncoding)
from utils import dnn_utils
from utils.batch_norm import BatchNormalizer


class DNN_model_with_embedding():
    """Deep neural network model with entity embeddings.
    """
    def __init__(self, name, train_df, valid_df, log_dir, save_dir,
                 emb_list, no_emb_quanti_list, no_emb_quali_list,
                 emb_size, ema_decay, bn_epsilon):
        self.name = name
        self.train_df = train_df
        self.valid_df = valid_df
        self.log_dir = log_dir
        self.save_dir = save_dir
        self.emb_list = emb_list
        self.no_emb_quanti_list = no_emb_quanti_list
        self.no_emb_quali_list = no_emb_quali_list
        self.n_emb = len(self.emb_list)
        self.n_no_emb_quanti = len(self.no_emb_quanti_list)
        self.n_no_emb_quali = len(self.no_emb_quali_list)
        self.emb_size = emb_size
        self.ema_decay = ema_decay
        self.bn_epsilon = bn_epsilon
        self.initialize_graph()

    def initialize_graph(self):
        """Initialize the graph model.
        """
        dnn_utils.create_logs_and_checkpoints_folders(self.log_dir,
                                           self.save_dir, self.name)
        tf.reset_default_graph()
        self.make_y_array()
        self.y = tf.placeholder(tf.float32,
                                shape=[None, 1], name='y')
        self.is_train = tf.placeholder(tf.bool,
                                       shape=[], name='train_flag')

        self.validation_loss = tf.placeholder(tf.float32, shape=[],
                                              name='validation_loss')
        self.validation_auc = tf.placeholder(tf.float32, shape=[],
                                             name='validation_auc')
        self.global_step = tf.Variable(0, trainable=False, dtype=tf.int64,
                                       name='global_step')
        # embedding part
        self.make_emb_array()
        self.X_emb = tf.placeholder(tf.int32, shape=[None, self.n_emb],
                                    name='X_emb')
        self.get_embedding_layer()
        # no embedding quanti part
        self.make_no_emb_quanti_array()
        self.X_no_emb_quanti = tf.placeholder(tf.float32,
                                              shape=[None, self.n_no_emb_quanti],
                                              name='X_no_emb_quanti')
        # no embedding quali part if exists
        if self.n_no_emb_quali:
            self.make_no_emb_quali_array()
            self.X_no_emb_quali = tf.placeholder(tf.float32,
                                                 shape=[None, self.n_no_emb_quali],
                                                 name='X_no_emb_quali')

            self.layers = tf.concat([self.emb_layer, self.X_no_emb_quanti,
                                     self.X_no_emb_quali], axis=1)
        else:
            self.layers = tf.concat([self.emb_layer, self.X_no_emb_quanti],
                                    axis=1)
        self.layers = [self.layers]

    def make_emb_array(self):
        """Apply transformations to input to return emb_array.
        """
        self.emb_pipeline = Pipeline([
            ('col_selector', DataFrameSelectColumns(self.emb_list)),
            ('lab_encoder', DataFrameConvertLabelEncoding())
        ])
        self.emb_train_array = self.emb_pipeline.fit_transform(
            self.train_df).values
        self.emb_valid_array = self.emb_pipeline.transform(
            self.valid_df).values

    def make_no_emb_quanti_array(self):
        """Apply transformations to input to return no_emb_quanti_array.
        """
        self.no_emb_quanti_pipeline = Pipeline([
            ('col_selector', DataFrameSelectColumns(self.no_emb_quanti_list)),
            ('num_encoder', DataFrameConvertNumeric()),
            ('std_scaler', StandardScaler())
        ])
        self.no_emb_quanti_train_array = self.no_emb_quanti_pipeline.fit_transform(
            self.train_df)
        self.no_emb_quanti_valid_array = self.no_emb_quanti_pipeline.transform(
            self.valid_df)

    def make_no_emb_quali_array(self):
        """Apply transformations to input to return no_emb_quali_array.
        """
        self.no_emb_quali_pipeline = Pipeline([
            ('col_selector', DataFrameSelectColumns(self.no_emb_quali_list)),
            ('num_encoder', DataFrameConvertLabelEncoding()),
            ('std_scaler', StandardScaler())
        ])
        self.no_emb_quali_train_array = self.no_emb_quali_pipeline.fit_transform(
            self.train_df)
        self.no_emb_quali_valid_array = self.no_emb_quali_pipeline.transform(
            self.valid_df)

    def make_y_array(self):
        """Return the target y in array with shape [None, 1].
        """
        self.y_train_array = self.train_df.ict1.values.astype(
            int).reshape(-1, 1)
        self.y_valid_array = self.valid_df.ict1.values.astype(
            int).reshape(-1, 1)

    def get_embedding_layer(self):
        """Concat all embedding tensors in one embedding layer.
        """
        with tf.name_scope("embeddings"):
            emb_matrix = tf.get_variable('E',
                                    [3000, self.emb_size],
                                    initializer=tf.random_uniform_initializer(
                                                     -0.08, 0.08))
            self.define_weight_summaries(emb_matrix)
            self.emb_layer = tf.nn.embedding_lookup(emb_matrix, self.X_emb)
            self.emb_layer = tf.reshape(self.emb_layer,
                                        [-1, self.n_emb * self.emb_size])

    def get_batch_data(self, batch_size):
        """Get train data batch.
        """
        indices = np.random.randint(0,
                                    len(self.train_df), batch_size)
        y_batch = self.y_train_array.take(indices, axis=0)
        X_emb_batch = self.emb_train_array.take(indices,
                                                axis=0)
        X_no_emb_quanti_batch = self.no_emb_quanti_train_array.take(indices,
                                                                    axis=0)
        if self.n_no_emb_quali:
            X_no_emb_quali_batch = self.no_emb_quali_train_array.take(indices,
                                                      axis=0)
            return X_emb_batch, X_no_emb_quanti_batch, X_no_emb_quali_batch, y_batch
        else:
            return X_emb_batch, X_no_emb_quanti_batch, y_batch


    def add_fc(self, name, size, weight_initializer, init_bias):
        """Add fully-connected layer.
        """
        with tf.variable_scope(name):
            input_features = self.layers[-1].get_shape().as_list()[1]
            weights = tf.get_variable('W', [input_features, size],
                                      initializer=weight_initializer)
            self.define_weight_summaries(weights)
            bias = tf.get_variable('b', [size],
                                   initializer=Constant(init_bias))
            fc = tf.matmul(self.layers[-1], weights) + bias
            self.layers.append(fc)

    def add_activation(self, activation):
        """Add activation to tensor.
        """
        if activation == 'elu':
            self.layers.append(tf.nn.elu(self.layers[-1]))
        elif activation == 'relu':
            self.layers.append(tf.nn.relu(self.layers[-1]))
        elif activation == 'sigmoid':
            self.layers.append(tf.nn.sigmoid(self.layers[-1]))

    def add_dropout(self, dropout):
        """Add dropout to tensor.
        """
        keep_prob = tf.cond(self.is_train,
                            lambda: tf.constant(1-dropout, tf.float32),
                            lambda : tf.constant(1, tf.float32),
                            name='keep_prob')
        self.layers.append(tf.nn.dropout(self.layers[-1], keep_prob))

    def define_weight_summaries(self, var):
        """Define summaries for layer weights (FC and Embeddings).
        """
        mean = tf.reduce_mean(var)
        tf.summary.scalar("mean", mean)
        tf.summary.histogram("mean", mean)
        stddev = tf.sqrt(tf.reduce_sum(tf.square(var - mean)))
        tf.summary.scalar( "stdv" , stddev)

    def add_batch_norm(self, tensor):
        """Add batch normalization layer.
        """
        bn = BatchNormalizer(self, self.ema_decay, self.bn_epsilon
                              ).normalize(self.is_train)
        self.layers.append(bn)

    def add_loss(self, name="loss", l2=0):
        """Add cross entropy loss.
        """
        with tf.variable_scope(name):
            cross_entropy = tf.nn.sigmoid_cross_entropy_with_logits(
                labels=self.y, logits=self.layers[-2])
            self.loss = tf.reduce_mean(cross_entropy)
            if l2:
                regularizer = l2 * tf.add_n([tf.nn.l2_loss(v)
                                            for v
                                            in tf.trainable_variables()
                                            if ('bias' not in v.name) and
                                            ('gamma' not in v.name)])
                self.loss = self.loss + regularizer

    def add_adam_optimizer(self, name, init_learning_rate, decay):
        """Add Adam optimizer to minimize loss.
        """
        with tf.variable_scope(name):
            if decay:
                self.learning_rate = tf.div(init_learning_rate,
                                            tf.cast(self.global_step + 1,
                                                    tf.float32),
                                            name='learning_rate')
            else:
                self.learning_rate = tf.constant(init_learning_rate,
                                                 name='learning_rate')
            self.optimizer = tf.train.AdamOptimizer(
                self.learning_rate).minimize(self.loss, self.global_step)

    def add_summaries(self, name):
        """Create summaries operation.
        """
        with tf.variable_scope(name):
            tf.summary.scalar('train_loss', self.loss)
            tf.summary.scalar('learning_rate', self.learning_rate)
            self.train_summary_op = tf.summary.merge_all()
            s3 = tf.summary.scalar('validation_loss',
                                   self.validation_loss)
            s4 = tf.summary.scalar('valid_auc', self.validation_auc)
            self.valid_summary_op = tf.summary.merge([s3, s4])

    def initialize_session(self, restore):
        """Initialize a new session.
        """
        self.sess = tf.Session()
        self.coord = tf.train.Coordinator()
        self.threads = tf.train.start_queue_runners(sess=self.sess,
                                                    coord=self.coord)
        self.writer = tf.summary.FileWriter(os.path.join(self.log_dir,
                                                         self.name),
                                            self.sess.graph)
        self.saver = tf.train.Saver()
        self.saver_def = self.saver.as_saver_def()
        self.sess.run(tf.global_variables_initializer())
        ckpt = tf.train.get_checkpoint_state(os.path.join(self.save_dir,
                                                          self.name))
        if restore and ckpt and ckpt.model_checkpoint_path:
            self.saver.restore(self.sess, ckpt.model_checkpoint_path)
            print('Restored session ' + ckpt.model_checkpoint_path)

    def close_session(self):
        """Close current session.
        """
        print('Saving session ' + os.path.join(self.save_dir, self.name,
                                               'model.ckpt'))
        self.saver.save(self.sess, os.path.join(self.save_dir, self.name,
                                                'model.ckpt'))
        self.writer.close()
        self.coord.request_stop()
        self.coord.join(self.threads)
        self.sess.close()

    def train(self, n_batches, step_size, train_batch_size,
              valid_batch_size, save):
        """Train model for one epoch.
        """
        step_loss = 0
        init_step = self.sess.run(self.global_step)
        for i in range(init_step, init_step + n_batches):
            if self.n_no_emb_quali:
                X_emb_batch, X_no_emb_quanti_batch, X_no_emb_quali_batch, y_batch = self.get_batch_data(
                    train_batch_size)
                feed_dict = {self.X_emb: X_emb_batch,
                             self.X_no_emb_quanti: X_no_emb_quanti_batch,
                             self.X_no_emb_quali: X_no_emb_quali_batch,
                             self.y: y_batch,
                             self.is_train: True}
            else:
                X_emb_batch, X_no_emb_quanti_batch, y_batch = self.get_batch_data(
                    train_batch_size)
                feed_dict = {self.X_emb: X_emb_batch,
                             self.X_no_emb_quanti: X_no_emb_quanti_batch,
                             self.y: y_batch,
                             self.is_train: True}
            _, batch_loss, summary = self.sess.run([self.optimizer,
                                                    self.loss,
                                                    self.train_summary_op],
                                                    feed_dict=feed_dict)
            step_loss += batch_loss

            if i > step_size:
                self.writer.add_summary(summary, i)

            if (i+1) % step_size == 0:
                valid_loss, valid_auc, valid_acc = self.validate_loss(i,
                                                           valid_batch_size)
                print('-- Train loss = %f' % (step_loss / step_size))
                print('-- Valid loss = %f' % valid_loss)
                print('-- Valid auc = %f' % (valid_auc))
                print('-- Valid acc = %f' % (valid_acc))
                step_loss = 0
                """
                if save:
                    print('Saving session ' +
                          os.path.join(self.save_dir, self.name,
                                       'model.ckpt-%d' % (i+1)))
                    self.saver.save(self.sess,
                                    os.path.join(self.save_dir, self.name,
                                                 'model.ckpt'), (i+1))
                """

    def validate_loss(self, i, valid_batch_size):
        """Get total loss on the validation data.
        """
        total_valid_loss = 0
        total_valid_pred = []
        total_valid_true = []
        n_batches = int(len(self.valid_df) / valid_batch_size)
        for j in range(n_batches):
            y = self.y_valid_array[
                j*valid_batch_size: (j+1)*valid_batch_size]
            X_emb = self.emb_valid_array[
                j*valid_batch_size: (j+1)*valid_batch_size]
            X_no_emb_quanti = self.no_emb_quanti_valid_array[
                j*valid_batch_size: (j+1)*valid_batch_size]
            if self.n_no_emb_quali:
                X_no_emb_quali = self.no_emb_quali_valid_array[
                    j*valid_batch_size: (j+1)*valid_batch_size]
                feed_dict = {self.X_emb: X_emb,
                             self.X_no_emb_quanti: X_no_emb_quanti,
                             self.X_no_emb_quali: X_no_emb_quali,
                             self.y: y,
                             self.is_train: False}
            else:
                feed_dict = {self.X_emb: X_emb,
                             self.X_no_emb_quanti: X_no_emb_quanti,
                             self.y: y,
                             self.is_train: False}

            batch_valid_loss, batch_valid_pred = self.sess.run([
                self.loss, self.layers[-1]], feed_dict=feed_dict)
            total_valid_loss += batch_valid_loss
            total_valid_pred += batch_valid_pred.flatten().tolist()
            total_valid_true += y.flatten().tolist()
        total_valid_loss /= n_batches
        valid_auc = dnn_utils.auc(total_valid_true, total_valid_pred)
        correct = tf.equal(tf.round(total_valid_pred), total_valid_true)
        valid_acc = dnn_utils.acc(total_valid_true,
                                  np.round(total_valid_pred))
        summary = self.sess.run(self.valid_summary_op,
                                {self.validation_loss: total_valid_loss,
                                 self.validation_auc: valid_auc})
        self.writer.add_summary(summary, i+1)
        return total_valid_loss, valid_auc, valid_acc
