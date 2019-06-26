# -*- coding: utf-8 -*-

"""
Define the model
"""

import tensorflow as tf 

def build_model(is_training, inputs, params):
    """
    Compute logits of the model.
    --
    Args:
        is_training: (bool) whether we are training or not
        inputs: (dict) contains the inputs of the graph (images, labels)
        params: (Params) hyperparameters
    Returns:
        output: (tf.Tensor) output of the model
    """
    images = inputs['images']

    assert images.get_shape().as_list() == [None, params.image_sizel,
                                            params.image_sizeL, 1]

    out = images
    # Define the number of channels of each convolution
    # For each block, we do: 3*3 conv -> batch norm -> relu -> 2*2 maxpool
    num_channels = params.num_channels
    bn_momentum = params.bn_momentum
    
    channels = [num_channels, num_channels * 2,
                num_channels * 3, num_channels * 4]
                
    for i, c in enumerate(channels):
        with tf.variable_scope('block_{}'.format(i+1)):
            out = tf.layers.conv2d(out, c, 1, padding='same')
            if params.use_batch_norm:
                out = tf.layers.batch_normalization(out,
                         momentum=bn_momentum, training=is_training)
            out = tf.nn.elu(out)
            out = tf.layers.max_pooling2d(out, 2, 2)

    out_shape = out.get_shape().as_list()
    print("Shape of last conv layer : {}".format(out_shape))
    out = tf.reshape(out, [-1, out_shape[1] * out_shape[2] * out_shape[3]])

    with tf.variable_scope('fc_1'):
        out = tf.layers.dense(out, num_channels * 6)
        if params.use_batch_norm:
            out = tf.layers.batch_normalization(out, 
                      momentum=bn_momentum, training=is_training)
        out = tf.nn.elu(out)
       
    with tf.variable_scope('fc_2'):
        logits = tf.layers.dense(out, params.num_labels)
    
    return logits

def model_fn(mode, inputs, params, reuse=False):
    """
    Model function defining the graph operations.
    --
    Args:
        mode: (string) can be 'train' or 'eval'
        inputs: (dict) contains the inputs of the graph
        params: (Params) contains hyperparameters of the model
        reuse: (bool) whether to reuse the weights
    
    Returns:
        model_spec: (dict) contains the graph operations or
                    nodes needed for training / evaluation
    """
    is_training = (mode == 'train')
    labels = inputs['labels']
    labels = tf.cast(labels, tf.int64)

    # Model : define the layers of the model
    with tf.variable_scope('model', reuse=reuse):
        # Compute the output distribution of the model
        # and the predictions
        logits = build_model(is_training, inputs, params)
        predictions = tf.argmax(logits, 1)
    
    # Define loss and accuracy
    loss = tf.losses.sparse_softmax_cross_entropy(
        labels=labels, logits=logits)
    accuracy = tf.reduce_mean(
        tf.cast(tf.equal(labels, predictions), tf.float32))

    # Define training step that minimizes 
    # the loss with the Adam Optimizer
    if is_training:
        optimizer = tf.train.AdamOptimizer(params.learning_rate)
        global_step = tf.train.get_or_create_global_step()
        if params.use_batch_norm:
            # Add a dependency to update the moving mean
            # and variance before performing
            # the train_op for batch normalization
            with tf.control_dependencies(
                tf.get_collection(tf.GraphKeys.UPDATE_OPS)):
                train_op = optimizer.minimize(loss,
                                              global_step=global_step)
        else:
            train_op = optimizer.minimize(loss,
                                          global_step=global_step)
    
    # Metrics and summaries
    # Metrics for evaluation using tf.metrics
    # (average over whole dataset)
    with tf.variable_scope('metrics'):
        metrics = {
            'accuracy': tf.metrics.accuracy(labels=labels,
                                            predictions=tf.argmax(logits, 1)),
            'loss': tf.metrics.mean(loss)
        }
    
    # Group the update ops for the tf.metrics
    update_metrics_op = tf.group(*[op for _, op in metrics.values()])

    # Get the op to reset the local variables used in tf.metrics
    metric_variables = tf.get_collection(
        tf.GraphKeys.LOCAL_VARIABLES, scope='metrics')
    metrics_init_op = tf.variables_initializer(metric_variables)

    # Summaries for training
    tf.summary.scalar('loss', loss)
    tf.summary.scalar('accuracy', accuracy)
    #tf.summary.image('train_image', inputs['images'])

    # Add incorrectly labeled images
    mask = tf.not_equal(labels, predictions)

    # Add a dfferent summary to know how they are misclassified
    for label in range(0, params.num_labels):
        mask_label = tf.logical_and(mask,
                                    tf.equal(predictions, label))
        incorrect_image_label = tf.boolean_mask(inputs['images'],
                                                mask_label)
        incorrect_filenames_label = tf.boolean_mask(inputs['filenames'],
                                                mask_label)
        tf.summary.image('incorrectly_labeled_{}'.format(label),
                         incorrect_image_label)
        tf.summary.text('incorrectly_labeled_{}'.format(label),
                         incorrect_filenames_label)

    # Model specification
    # Create the model specification and return it
    # It contains nodes or operations in the graph
    # that will be used for training and evaluation
    model_spec = inputs
    model_spec['variable_init_op'] = tf.global_variables_initializer()
    model_spec['predictions'] = predictions
    model_spec['loss'] = loss
    model_spec['accuracy'] = accuracy
    model_spec['metrics_init_op'] = metrics_init_op
    model_spec['metrics'] = metrics
    model_spec['update_metrics'] = update_metrics_op
    model_spec['summary_op'] = tf.summary.merge_all()

    if is_training:
        model_spec['train_op'] = train_op

    return model_spec




