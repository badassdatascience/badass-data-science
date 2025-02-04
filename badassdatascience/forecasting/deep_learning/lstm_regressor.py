#
# Load useful libraries
#
import os
import pickle
import uuid
import json

from numpy.random import seed

import tensorflow
from tensorflow.random import set_seed

from keras import layers
from keras.models import Sequential
from keras import regularizers
from keras.callbacks import ReduceLROnPlateau
from keras.callbacks import ModelCheckpoint
from keras.optimizers import Adam

from numba import cuda 

#
# Load the run configuration, setting the
# training ID in the process
#
from config_lstm_regressor import *








#
# Reset device
#
device = cuda.get_current_device()
device.reset()

#
# set seeds
#
seed(config['numpy_seed'])
set_seed(config['tensorflow_seed'])

#
# Load data
#
with open(config['data_source_path'] + '/' + uid_data + '_train_val_test_dict.pickled', 'rb') as f:
    train_val_test_dict = pickle.load(f)

    #QA
    print(train_val_test_dict['train']['M'].shape)
    print(train_val_test_dict['train']['y'].shape)
    
M = train_val_test_dict['train']['M']
y = train_val_test_dict['train']['y_forward']

#
# calculate input and output matrix/array shapes
#
config['calculated_input_shape'] = (M.shape[1], M.shape[2])
config['calculated_number_of_outputs'] = y.shape[1]

#
# save configuration
#
with open(config['json_config_output_path'], 'w') as f:
    json.dump(config, f, indent = 2)

#
# build a generic Keras LSTM regressor
#
def build_generic_LSTM_regressor(**config):
    model = Sequential()

    #
    # build RNN layers (this will always produce at least one, optionally more)
    #
    for i, n_units_in_layer in enumerate(config['number_of_cells_per_RNN_layer_list']):

        if i == 0:

            #
            # define input layer
            #
            model.add(
                layers.LSTM(
                    n_units_in_layer,
                    return_sequences = True,
                    input_shape = config['calculated_input_shape'],
                    recurrent_dropout = config['rnn_recurrent_dropout_rate'],
                )
            )

        else:
            model.add(
                layers.LSTM(
                    n_units_in_layer,
                    return_sequences = True,
                    recurrent_dropout = config['rnn_recurrent_dropout_rate'],
                )
            )
                      
        # EW

        if config['lstm_activation_function'] == 'LeakyReLU':
            model.add(layers.LeakyReLU())
        else:
            model.add(layers.ReLU())

        if config['use_batch_normalization_layers']:
            model.add(layers.BatchNormalization(momentum = config['batch_normalization_momentum'])) 

        if config['use_dropout_layers']:
            model.add(layers.Dropout(rate = config['rnn_dropout_rate'],))
        
    #
    # flatten
    #
    model.add(layers.Flatten())

    #
    # first batch normalization
    #
    if config['use_batch_normalization_layers']:
        model.add(layers.BatchNormalization(momentum = config['batch_normalization_momentum']))
    
    #
    # Build dense layers
    #
    for n_units_in_layer in config['number_of_cells_per_dense_layer_list']:

        model.add(
            layers.Dense(
                n_units_in_layer,

                # https://keras.io/api/layers/regularizers/
                kernel_regularizer = regularizers.L1L2(
                    config['regularizer_kernel_L1'],
                    config['regularizer_kernel_L2'],
                ),
                bias_regularizer = regularizers.L2(config['regularizer_bias_L2']),
                activity_regularizer = regularizers.L2(config['regularizer_activity_L2']),
            )
        )

        # EW
        if config['dense_activation_function'] == 'LeakyReLU':
            model.add(layers.LeakyReLU())
        else:
            model.add(layers.ReLU())

        if config['use_batch_normalization_layers']:
            model.add(layers.BatchNormalization(momentum = config['batch_normalization_momentum']))

        if config['use_dropout_layers']:
            model.add(layers.Dropout(rate = config['dense_dropout_rate'],))

    #
    # define output layer
    #
    model.add(
        layers.Dense(
            config['calculated_number_of_outputs'],

            # https://keras.io/api/layers/regularizers/
            kernel_regularizer = regularizers.L1L2(
                config['regularizer_kernel_L1'],
                config['regularizer_kernel_L2'],
            ),
            bias_regularizer = regularizers.L2(config['regularizer_bias_L2']),
            activity_regularizer = regularizers.L2(config['regularizer_activity_L2']),
        )
    )

                  
    # EW
    if config['final_dense_activation_function'] == 'LeakyReLU':
        model.add(layers.LeakyReLU())
    else:
        model.add(layers.ReLU())

    if config['use_batch_normalization_layers']:
        model.add(layers.BatchNormalization(momentum = config['batch_normalization_momentum'])) 

    if config['use_dropout_layers']:
        model.add(layers.Dropout(rate = config['dense_dropout_rate'],))
    
    return model

#
# compile the generic Keras LSTM regressor given above
#
def compile_generic_regressor(model, **config): #loss = 'mse', metrics = ['mse']):
    model.compile(
        optimizer = Adam(learning_rate = config['learning_rate']),
        loss = config['loss_function'],
        metrics = config['metrics_to_store'],
        )

#
# fit the generic Keras LSTM classifer given above
#
def fit_generic_regressor(model, train_X, train_y, **config):

    #
    # set callbacks list
    #
    callbacks_list = [
        ReduceLROnPlateau(
            monitor = config['callbacks_dict']['ReduceLROnPlateau']['monitor'],
            factor = config['callbacks_dict']['ReduceLROnPlateau']['factor'],
            patience = config['callbacks_dict']['ReduceLROnPlateau']['patience'],
        ),
        ModelCheckpoint(
            filepath = config['checkpoint_file_path'],
            monitor = config['model_checkpoint_monitor'],
            save_best_only = config['model_checkpoint_save_best_only'],
        ),
    ]

    print()
    print(train_X.shape)
    print(train_y.shape)
    print()

    if config['use_variable_learning_rate']:
        history = model.fit(
            train_X,
            train_y,
            validation_split = config['validation_split'],
            epochs = config['epochs'],
            batch_size = config['batch_size'],
            callbacks = callbacks_list,
        )
    else:
        history = model.fit(
            train_X,
            train_y,
            validation_split = config['validation_split'],
            epochs = config['epochs'],
            batch_size = config['batch_size'],
        )
        

    return history

#
# build model
#
model = build_generic_LSTM_regressor(**config)

#
# save model to JSON
#
model_json = model.to_json()
f = open(config['model_json_path'], 'w')
f.write(model_json)
f.close()

#
# compile model
#
compile_generic_regressor(
    model,
    **config,
)

#
# fit model
#
history = fit_generic_regressor(
    model,
    M,
    y,
    **config,
)

#
# save final weights
#
model.save_weights(config['model_final_weights_path'])

#
# save history
#
with open(config['model_final_history_path'], 'wb') as f:
    pickle.dump(history.history, f)

#
# display unique ID
#
print()
print(uid_data + '----' + uid_training)
print()

#
# reset device (again)
#
device = cuda.get_current_device()
device.reset()
