# -*- coding: utf-8 , python 3 -*-
# Author: Maylis Cotadze
# Challenge Machine Learning Telecom ParisTech - MDI341 - 04-2017

import os
import time

import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import tensorflow as tf
from sklearn.preprocessing import StandardScaler
from sklearn.utils import shuffle

# Create checkpoints folder
save_path = 'checkpoints/'
model_name = 'cnn'
if not os.path.exists(save_path):
    os.makedirs(save_path)

BATCH_SIZE = 100
SKIP_STEP = 10
DROPOUT = 1
N_EPOCHS = 5

# Step 1: Read in data
images_train_fname    = './data/data_train.bin'
templates_train_fname = './data/fv_train.bin'

images_valid_fname    = './data/data_valid.bin'
templates_valid_fname = './data/fv_valid.bin'

images_test_fname     = './data/data_test.bin'

# number of images
num_train_images = 100000
num_valid_images = 10000
num_test_images  = 10000

# size of the images 48*48 pixels in gray levels
image_dim = 48 * 48

# dimension of the templates
template_dim = 128

# Load and normalize data
def load_normalize_shuffle_data():
    # read the training files
    with open(templates_train_fname, 'rb') as f:
        train_template_data = np.fromfile(f, dtype=np.float32, \
        count=num_train_images * template_dim)
        train_template_data = train_template_data.reshape(num_train_images, \
        template_dim)

    with open(images_train_fname, 'rb') as f:
        train_image_data = np.fromfile(f, dtype=np.uint8, \
                        count=num_train_images * image_dim).astype(np.float32)
        train_image_data = train_image_data.reshape(num_train_images, image_dim)

    # read the validation files
    with open(templates_valid_fname, 'rb') as f:
        valid_template_data = np.fromfile(f, dtype=np.float32, \
                                count=num_valid_images * template_dim)
        valid_template_data = valid_template_data.reshape(num_valid_images,\
                                template_dim)

    with open(images_valid_fname, 'rb') as f:
        valid_image_data = np.fromfile(f, dtype=np.uint8, \
                            count=num_valid_images* image_dim).astype(np.float32)
        valid_image_data = valid_image_data.reshape(num_valid_images, image_dim)

    # read the test file
    with open(images_test_fname, 'rb') as f:
        test_image_data = np.fromfile(f, dtype=np.uint8, \
                            count=num_test_images * image_dim).astype(np.float32)
        test_image_data = test_image_data.reshape(num_test_images, image_dim)

    # Center data
    train_image_data = StandardScaler(with_std=True).fit_transform(train_image_data)
    valid_image_data =  StandardScaler(with_std=True).fit_transform(valid_image_data)
    test_image_data =  StandardScaler(with_std=True).fit_transform(test_image_data)
    # Shuffle data
    train_image_data, train_template_data = shuffle(train_image_data,\
                                            train_template_data, random_state=0)

    return train_image_data, train_template_data, valid_image_data, \
            valid_template_data, test_image_data

# scoring function
def compute_pred_score(y_true, y_pred):
    err_y = np.mean((y_true - y_pred) ** 2)
    return err_y



class ConvNetChal:
    """ Build the graph for challenge convnet model """
    def __init__(self, batch_size, skip_step):
        self.batch_size = batch_size
        self.skip_step = skip_step
        self.global_step = tf.Variable(0, dtype=tf.int32, trainable=False, \
                            name='global_step')

    def _create_placeholders(self):
        """ Step 1: define the placeholders for input, output and dropout"""
        # each image in the data is of shape 48*48 = 2304
        with tf.name_scope("data"):
            self.X = tf.placeholder(tf.float32, shape=[None, 2304],\
                    name="X_placeholder")
            self.Y = tf.placeholder(tf.float32, shape=[None, 128], \
                    name="Y_placeholder")
            self.dropout = tf.placeholder(tf.float32, name='dropout')

    def _create_layers(self):
        """ Step 2: define weights + model inference"""

        with tf.device('/cpu:0'):
            with tf.name_scope("conv1"):
                self.images = tf.reshape(self.X, shape=[-1, 48, 48, 1])
                self.kernel1 = tf.Variable(tf.random_normal(shape=[8, 8, 1, 14],\
                                stddev=np.sqrt(1/(48*48))), name='kernels', \
                                dtype=tf.float32)
                self.biases = tf.Variable(tf.zeros([14]), name='biases', \
                                dtype=tf.float32)
                self.conv = tf.nn.conv2d(self.images, self.kernel1, \
                                strides=[1, 1, 1, 1], padding='SAME')
                self.conv1 = tf.nn.elu(self.conv + self.biases, name='conv1')
                # output is of dimension BATCH_SIZE x 48 x 48 x 14

            with tf.variable_scope("conv2"):
                self.kernel2 = tf.Variable(tf.random_normal(shape=[6, 6, 14, 16],\
                                stddev=np.sqrt(1/(48*48*14))), name='kernels', \
                                dtype=tf.float32)
                self.biases = tf.Variable(tf.zeros([16]), name='biases', \
                                dtype=tf.float32)
                self.conv = tf.nn.conv2d(self.conv1, self.kernel2, \
                                strides=[1, 1, 1, 1], padding='SAME')
                self.conv2 = tf.nn.elu(self.conv + self.biases, name='conv2')
                # output is of dimension BATCH_SIZE x 48 x 48 x 16

            with tf.variable_scope("pool1"):
                self.pool = tf.nn.max_pool(self.conv2, ksize=[1, 2, 2, 1], \
                                strides=[1, 2, 2, 1],padding='SAME')
                self.pool1 = tf.nn.dropout(self.pool, self.dropout)
                # output is of dimension BATCH_SIZE x 24 x 24 x 16

            with tf.variable_scope("conv3"):
                self.kernel3 = tf.Variable(tf.random_normal(shape=[6, 6, 16, 18],\
                                stddev=np.sqrt(1/(24*24*16))), name='kernels', \
                                dtype=tf.float32)
                self.biases = tf.Variable(tf.zeros([18]), name='biases', \
                                dtype=tf.float32)
                self.conv = tf.nn.conv2d(self.pool1, self.kernel3, \
                                strides=[1, 1, 1, 1], padding='SAME')
                self.conv3 = tf.nn.elu(self.conv + self.biases, name='conv3')
                # output is of dimension BATCH_SIZE x 24 x 24 x 18

            with tf.variable_scope("conv4"):
                self.kernel4 = tf.Variable(tf.random_normal(shape=[4, 4, 18, 6],\
                                stddev=np.sqrt(1/(24*24*18))), name='kernels', \
                                dtype=tf.float32)
                self.biases = tf.Variable(tf.zeros([6]), name='biases', \
                                dtype=tf.float32)
                self.conv = tf.nn.conv2d(self.conv3, self.kernel4, \
                                        strides=[1, 1, 1, 1], padding='SAME')
                self.conv4 = tf.nn.elu(self.conv + self.biases, name='conv4')
                # output is of dimension BATCH_SIZE x 24 x 24 x 6

            with tf.variable_scope("pool2"):
                self.pool = tf.nn.max_pool(self.conv4, ksize=[1, 2, 2, 1], \
                                strides=[1, 2, 2, 1],padding='SAME')
                self.pool2 = tf.nn.dropout(self.pool, self.dropout)
                # output is of dimension BATCH_SIZE x 12 x 12 x 6

            with tf.variable_scope("conv5"):
                self.kernel5 = tf.Variable(tf.random_normal(shape=[4, 4, 6, 6],\
                                stddev=np.sqrt(1/(12*12*6))), name='kernels', \
                                dtype=tf.float32)
                self.biases = tf.Variable(tf.zeros([6]), name='biases', \
                                dtype=tf.float32)
                self.conv = tf.nn.conv2d(self.pool2, self.kernel5, \
                                strides=[1, 2, 2, 1], padding='SAME')
                self.conv5 = tf.nn.elu(self.conv + self.biases, name='conv5')
                # output is of dimension BATCH_SIZE x 6 x 6 x 6


            with tf.variable_scope("fc1"):
                input_features = 6 * 6 * 6
                self.w = tf.Variable(tf.random_normal(shape=[input_features, 128], \
                                stddev=0.001), name='weights', dtype = tf.float32)
                b = tf.Variable(tf.zeros([128]), name='biases')
                self.conv = tf.reshape(self.conv5, [-1, input_features])
                self.fc = tf.matmul(self.conv, self.w) + b

    def _create_loss(self):
        """ Step 3 : define the loss function """
        with tf.device('/cpu:0'):
            with tf.name_scope("loss"):
                self.loss = tf.reduce_mean(tf.square(self.Y - self.fc,\
                            name ='loss'))

    def _create_learning_rate(self):
        with tf.device('/cpu:0'):
            with tf.name_scope("learning_rate"):
                self.lr = tf.train.exponential_decay(0.0008, self.global_step, \
                                                    100, 0.95, staircase=True)


    def _create_optimizer(self):
        """ Step 4 : define optimizer """
        with tf.device('/cpu:0'):
            self.optimizer = tf.train.AdamOptimizer(self.lr).minimize(self.loss,\
                            global_step=self.global_step)

    def define_summaries(self, var):
        mean = tf.reduce_mean(var)
        tf.summary.scalar( "mean", mean)
        tf.summary.histogram("mean", mean)
        stddev = tf.sqrt(tf.reduce_sum(tf.square(var - mean)))
        tf.summary.scalar( "stdv" , stddev)
        tf.summary.histogram("stdv", stddev)

    def _create_summaries(self):
        with tf.name_scope("summaries"):
            self.define_summaries(self.kernel1)
            self.define_summaries(self.kernel2)
            self.define_summaries(self.kernel3)
            self.define_summaries(self.kernel4)
            self.define_summaries(self.kernel5)
            self.define_summaries(self.w)
            tf.summary.scalar("loss", self.loss)
            tf.summary.histogram("histogram_loss", self.loss)
            tf.summary.scalar("learning_rate", self.lr)
            self.summary_op = tf.summary.merge_all()

    def build_graph(self):
        """ Build the graph for our model """
        self._create_placeholders()
        self._create_layers()
        self._create_loss()
        self._create_learning_rate()
        self._create_optimizer()
        self._create_summaries()

def train_model(valid, model, train_image_data, train_template_data, valid_image_data,\
                valid_template_data, test_image_data,batch_size, n_epochs, \
                dropout, skip_step):

    saver = tf.train.Saver() # defaults to saving all variables -
    save_path_full = os.path.join(save_path, model_name)
    initial_step = 0
    with tf.Session() as sess:

        sess.run(tf.global_variables_initializer())
        ckpt = tf.train.get_checkpoint_state(os.path.dirname('checkpoints/checkpoint'))
        # if that checkpoint exists, restore from checkpoint
        if ckpt and ckpt.model_checkpoint_path:
            print(ckpt.model_checkpoint_path)
            saver.restore(sess, ckpt.model_checkpoint_path)
            print("Model restored.")

        total_loss = 0.0 # we use this to calculate late average loss in the last SKIP_STEP steps
        writer = tf.summary.FileWriter('graph/lr', sess.graph)
        initial_step = model.global_step.eval()
        n_batches = int(train_template_data.shape[0] / batch_size)
        start_time = time.time()
        # 50 = n_batches * n_epochs
        for index in range(initial_step, n_batches * n_epochs):

            offset = (index * BATCH_SIZE) % (train_template_data.shape[0] - batch_size)
            X_batch = train_image_data[offset:(offset + batch_size), :]
            Y_batch = train_template_data[offset:(offset + batch_size), :]
            feed_dict = {model.X : X_batch, model.Y : Y_batch, \
                            model.dropout : 1}
            loss_batch, _, summary = sess.run([model.loss, model.optimizer, \
                            model.summary_op],feed_dict=feed_dict)
            writer.add_summary(summary, global_step=index)
            total_loss += loss_batch
            if (index + 1) % skip_step == 0:
                print('Average loss at step {}: {}'.format(index, \
                total_loss / skip_step))
                total_loss = 0.0
                saver.save(sess, './checkpoints/cnn1', global_step = index)

        print("Optimization Finished !")
        print("Total time: {0} seconds".format(time.time() - start_time))

        if valid:
            pred = sess.run([model.fc], feed_dict={model.X : valid_image_data,\
                                model.dropout :1})

            print("Validation score : {}".format(compute_pred_score(\
                                    valid_template_data, pred)))
        else :
            pred = sess.run([model.fc], feed_dict={model.X : test_image_data,\
                                model.dropout :1})

            test = pred[0]
            f = open('template_pred.bin', 'wb')
            for i in range(num_test_images):
                f.write(test[i, :])
            f.close()
        return pred

def main(load=False):
    train_image_data, train_template_data, valid_image_data, \
    valid_template_data, test_image_data = load_normalize_shuffle_data()

    model = ConvNetChal( BATCH_SIZE, SKIP_STEP, DROPOUT)
    model.build_graph()
    pred = train_model(True, model, train_image_data, train_template_data, \
                valid_image_data, valid_template_data, test_image_data,\
                 BATCH_SIZE, N_EPOCHS,DROPOUT, SKIP_STEP)


if __name__ == '__main__':
    main()

# tensorboard --logdir="./graph/lr" --port 6006
