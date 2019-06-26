# -*- coding: utf-8 -*-

"""
Create the input data pipeline using 'tf.data'
"""

import tensorflow as tf
import math
import random

def _parse_function(filename, label, sizel, sizeL):
    """
    Obtain the image from the filename.

    The following operations are applied:
        - Decode the image from jpeg format
        - Convert to float and to range [0, 1]
    """
    image_string = tf.read_file(filename)

    image_decoded = tf.image.decode_jpeg(image_string, channels=3)

    image_grayscale = tf.image.rgb_to_grayscale(image_decoded)

    # This will convert to float valies in [0, 1]
    image = tf.image.convert_image_dtype(image_grayscale, tf.float32)

    resized_image = tf.image.resize_images(image, [sizel, sizeL])

    return resized_image, label, filename



def train_preprocess(image, label, filename, use_random_flip):
    """
    Image preprocessing for training.

    Apply the following operations:
        - Vertically flip the image with probability 1/2
        - Apply random brightness and saturation
    """
    if use_random_flip:
        image = tf.image.random_flip_up_down(image)
    
    image = tf.image.random_brightness(image, max_delta=0.15)
    #image = tf.image.random_saturation(image, lower=0.5, upper=1.5)

    # Make sure the image is still in [0, 1]
    image = tf.clip_by_value(image, 0.0, 1.0)

    return image, label, filename

def input_fn(is_training, filenames, labels, params):
    """
    Input function.

    The filenames have format "{label}_...._.jpeg"
    --
    Args:
        is_training: (bool) wether to use the train or test pipeline.
                      At training, we shuffle the data and have multiple epochs. 
        filenames: (list) filenames of the images
        labels: (list) corresponding list of labels
        params: (Params) contains hyperparameters of the model.
    """
    num_samples = len(filenames)
    assert len(filenames) == len(labels), 'Filenames and labels should have same length'

    # Create a Dataset serving batches of images and labels
    parse_fn = lambda f, l: _parse_function(f, l, params.image_sizel, params.image_sizeL)
    train_fn = lambda i, l, f: train_preprocess(i, l, f, params.use_random_flip)

    if is_training:
        dataset = (tf.data.Dataset.from_tensor_slices((tf.constant(filenames),
                                                       tf.constant(labels)))
            .shuffle(num_samples)
            .map(parse_fn, num_parallel_calls=params.num_parallel_calls)
            .map(train_fn, num_parallel_calls=params.num_parallel_calls)
            .batch(params.batch_size)
            .prefetch(1)
        )
    else:
        dataset = (tf.data.Dataset.from_tensor_slices((tf.constant(filenames),
                                                       tf.constant(labels)))
            .map(parse_fn)
            .batch(params.batch_size)
            .prefetch(1)
        )
    
    # Create reinitializable iterator from dataset
    iterator = dataset.make_initializable_iterator()
    images, labels, filenames = iterator.get_next()
    iterator_init_op = iterator.initializer

    inputs = {'images': images, 'labels': labels, 'filenames': filenames,
              'iterator_init_op': iterator_init_op}
    
    return inputs