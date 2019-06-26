# -*- coding: utf-8 -*-

"""
Define the model
"""

import tensorflow as tf

def build_model(inputs, params):
    """
    Compute normalized_embeddings of the model.
    --- 
    Args:
        inputs: (dict) contains the inputs of the graph
                this can be 'tf.placeholder' or outputs of 'tf.data'
        params: (dict) contains hyperparameters of the model
    Returns:
        input_embeddings: (tf.Tensor) embedding matrix of the model
    """
    X = inputs['target']
    # Define an embedding matrix and an embedding lookup that can retrieve
    # the embedding for any given word in the dataset.
    embedding_matrix = tf.get_variable(name='embedding', dtype=tf.float32,
                                       shape=[params['vocab_size'],
                                              params['embedding_size']])
    
    input_embeddings = tf.nn.embedding_lookup(embedding_matrix, X)

    norm = tf.sqrt(tf.reduce_sum(tf.square(embedding_matrix), 1,
                                 keepdims=True))
    normalized_embedding_matrix = embedding_matrix / norm
    
    return input_embeddings, normalized_embedding_matrix


def compute_cosine_similarity(normalized_embedding_matrix,
                              inputs):
    """
    Compute the cosine similarity between the batch examples
    and all embeddings.
    --- 
    Args:
        normalized_embedding_matrix: (tf.Tensor) embedding matrix
        input_embeddings: batch embeddings
    """
    X = inputs['target']
    valid_embeddings = tf.nn.embedding_lookup(normalized_embedding_matrix,
                                              X)
    similarity = tf.matmul(
        valid_embeddings, normalized_embedding_matrix, transpose_b=True)
    
    return similarity


def model_fn(mode, inputs, params, reuse=False):
    """
    Model function defining the graph operations.
    ---
    Args:
        mode: (string) 'train', 'eval'
        inputs: (dict) contains the inputs of the graph
                this can be 'tf.placeholder' or outputs of 'tf.data'
        params: (dict) contains hyperparameters of the model
        reuse: (bool) wether to reuse the weights
    Returns:
        model_spec: (dict) contains the graph operations or 
                    nodes needed for training / evaluation
    """
    is_training = (mode == 'train')
    Y = tf.reshape(inputs['context'], shape=[params['batch_size'], 1])

    # MODEL : define the layers of the model
    with tf.variable_scope('model', reuse=reuse):
        input_embeddings, normalized_embedding_matrix = build_model(inputs,
                                                                    params)
    """
    # Compute the cosine similarity between batch examples and all embeddings
    with tf.variable_scope('similarity', reuse=reuse):
        if is_training:
            similarity = None
        else:
            similarity = compute_cosine_similarity(normalized_embedding_matrix,
                                                   inputs)
    """

    # Define the noise-contrastive estimation loss (logistic regression model)
    # First, construct the variables for the NCE loss:
    # define the output weights and biases for each word of the vocab
    with tf.variable_scope('nce_weights', reuse=reuse):
        nce_weights = tf.get_variable(name='nce_weights', dtype=tf.float32,
                                      shape=[params['vocab_size'], 
                                      params['embedding_size']])

        nce_biases = tf.get_variable(name='nce_bias', 
                                     initializer=tf.zeros(params['vocab_size']))
    # Compute the average NCE loss for the batch
    # tf.nce_loss automatically draws a new sample of the negative labels for
    # each time we evaluate the loss.
    loss = tf.reduce_mean(
        tf.nn.nce_loss(
            weights=nce_weights,
            biases=nce_biases,
            labels=Y,
            inputs=input_embeddings,
            num_sampled=params['num_sampled'],
            num_classes=params['vocab_size'])
        )

    # Define training step that minimize the loss with SGD optimizer
    if is_training:
        optimizer = tf.train.GradientDescentOptimizer(params['learning_rate'])
        global_step = tf.train.get_or_create_global_step()
        train_op = optimizer.minimize(loss, global_step=global_step)
    
    # METRICS AND SUMMARIES
    # Metrics for evaluation using tf.metrics (average over whole dataset)
    with tf.variable_scope('metrics'):
        metrics = {
            'loss': tf.metrics.mean(loss)
        }

    # Group the updates ops for the tf.metrics
    update_metrics_op = tf.group(*[op for _, op in metrics.values()])

    # Get the op to reset the local variables used in tf.metrics
    metric_variables = tf.get_collection(tf.GraphKeys.LOCAL_VARIABLES,
                                         scope='metrics')
    metrics_init_op = tf.variables_initializer(metric_variables)

    # Summaries for training
    tf.summary.scalar('loss', loss)

    # MODEL SPECIFICATION
    # Create the model specification and return it : it contains nodes
    # or operations in the graph that will be used for training and evaluation
    model_spec = inputs
    variable_init_op = tf.group(*[tf.global_variables_initializer(),
                                  tf.tables_initializer()])
    model_spec['variable_init_op'] = variable_init_op
    model_spec['input_embeddings'] = input_embeddings
    model_spec['normalized_embedding_matrix'] = normalized_embedding_matrix
    #model_spec['similarity'] = similarity
    model_spec['loss'] = loss
    model_spec['metrics_init_op'] = metrics_init_op
    model_spec['metrics'] = metrics
    model_spec['update_metrics'] = update_metrics_op
    model_spec['summary_op'] = tf.summary.merge_all()

    if is_training:
        model_spec['train_op'] = train_op
    
    return model_spec