# -*- coding: utf-8 -*-

###
# DNN Model with entity embeddings - SRE V2
###


import sys
import os
import logging
import numpy as np
import pandas as pd
import tensorflow as tf
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler
from tensorflow.python.ops.init_ops import UniformUnitScaling, Constant

from utils import dnn_utils
from utils.batch_norm import BatchNormalizer


class DNN_model_with_embedding():
    """Deep neural network model with entity embeddings.
    """
    def __init__(self, name, train_df, log_dir, save_dir,
                 emb_list, no_emb_list, emb_size, ema_decay, bn_epsilon):
        self.name = name
        self.train_df = train_df
        self.log_dir = log_dir
        self.save_dir = save_dir
        self.emb_list = emb_list
        self.no_emb_list = no_emb_list
        self.n_emb = len(self.emb_list)
        self.n_no_emb = len(self.no_emb_list)
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
        self.embedding_config()
        self.X_emb = tf.placeholder(tf.int32, shape=[None, self.n_emb],
                                    name='X_emb')
        self.get_embedding_layer()
        # no embedding part
        self.make_no_emb_array()
        self.X_no_emb = tf.placeholder(tf.float32, shape=[None, self.n_no_emb],
                                       name='X_no_emb')

        self.layer = tf.concat([self.emb_layer, self.X_no_emb], axis=1)
        self.layers = [self.layer]

    def make_emb_array(self):
        """Apply transformations to input to return emb_array.
        """
        self.emb_train_array = self.train_df[self.emb_list].astype(
            int).values

    def make_no_emb_array(self):
        """Apply transformations to input to return no_emb_array.
        """
        self.no_emb_train_array = self.train_df[self.no_emb_list].astype(
            float).values

    def make_y_array(self):
        """Return the target y in array with shape [None, 1].
        """
        self.y_train_array = self.train_df.ict1.values.astype(
            int).reshape(-1, 1)

    def embedding_config(self):
        """Define the size of the embedding tensor for each
        entity to embed.
        """
        self.emb_config = [len(set(self.emb_train_array[:, el])) 
                           for el in range(self.n_emb)]

    def get_embedding_tensor(self, tensor, emb_config, idx):
        """Get embedding tensor for each entity to embed.
        """
        input_size = emb_config[idx]
        emb_matrix = tf.get_variable('E' + str(idx),
                                     [input_size + 20, self.emb_size],
                                     initializer=tf.random_uniform_initializer(
                                         -0.08, 0.08))
        self.define_weight_summaries(emb_matrix)
        emb_tensor = tf.nn.embedding_lookup(emb_matrix, tensor[:, idx])
        return emb_tensor

    def get_embedding_layer(self):
        """Concat all embedding tensors in one embedding layer.
        """
        with tf.name_scope("embeddings"):
            self.tensor_list = [self.get_embedding_tensor(
                self.X_emb, self.emb_config, idx)
                for idx in range(self.n_emb)]
            self.emb_layer = tf.concat(self.tensor_list, axis=1)

    def get_batch_data(self, batch_size):
        """Get train data batch.
        """
        indices = np.random.randint(0,
                                    len(self.train_df), batch_size)
        y_batch = self.y_train_array.take(indices, axis=0)
        X_emb_batch = self.emb_train_array.take(indices, axis=0)
        X_no_emb_batch = self.no_emb_train_array.take(indices, axis=0)
        return X_emb_batch, X_no_emb_batch, y_batch

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
            logging.info('Restored session ' + ckpt.model_checkpoint_path)

    def close_session(self):
        """Close current session.
        """
        logging.info('Saving session ' + os.path.join(self.save_dir, self.name,
                                               'model.ckpt'))
        self.saver.save(self.sess, os.path.join(self.save_dir, self.name,
                                                'model.ckpt'))
        self.writer.close()
        self.coord.request_stop()
        self.coord.join(self.threads)
        self.sess.close()

    def train(self, n_batches, step_size, train_batch_size):
        """Train model for one epoch.
        """
        step_loss = 0
        init_step = self.sess.run(self.global_step)
        for i in range(init_step, init_step + n_batches):
            X_emb_batch, X_no_emb_batch, y_batch = self.get_batch_data(
                    train_batch_size)

            feed_dict = {self.X_emb: X_emb_batch,
                         self.X_no_emb: X_no_emb_batch,
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
                logging.info('-- Train loss = %f' % (step_loss / step_size))
                step_loss = 0

    def predict(self, test_df):
        """Get pred proba for test_df.
        """
        self.initialize_session(restore=True)
        X_emb = test_df[self.emb_list].astype(int).values
        X_no_emb = test_df[self.no_emb_list].astype(float).values
        feed_dict = {self.X_emb: X_emb,
                     self.X_no_emb: X_no_emb,
                     self.is_train: False}

        pred = self.sess.run([self.layers[-1]], feed_dict=feed_dict)
        self.close_session()
        return pred

    def save_model(self):
        """Save graph in .pb and variables in .index and .data-00000-of-00001
        for java predictions.
        """
        signature = tf.saved_model.signature_def_utils.build_signature_def(
            inputs = {'input1':
                      tf.saved_model.utils.build_tensor_info(self.X_emb),
                      'input2':
                      tf.saved_model.utils.build_tensor_info(self.X_no_emb)},
            outputs = {'output':
                       tf.saved_model.utils.build_tensor_info(self.layers[-1])},
        )
        saved_model_folder = './saved_model/{}'.format(self.name)
        if os.path.exists(saved_model_folder):
            if os.path.isdir(saved_model_folder):
                shutil.rmtree(saved_model_folder)
        builder = tf.saved_model.builder.SavedModelBuilder(saved_model_folder)
        builder.add_meta_graph_and_variables(self.sess,
            [tf.saved_model.tag_constants.SERVING],
             signature_def_map={
             tf.saved_model.signature_constants.DEFAULT_SERVING_SIGNATURE_DEF_KEY:
             signature})
        builder.save()
        logging.info("Model saved in folder saved_model")
        # write graph in readable format
        tf.train.write_graph(self.sess.graph_def, '.', os.path.join(
                            self.save_dir, self.name, 'trained_model.json'),
                            as_text=True)