#
# Specify the data source ID to use in this training run
#
# Perhaps this should be a command line argument.
#

# uid_data = '9e65a6e5-7926-422e-bbd0-3728e0dfe6b3'  # 120
#uid_data = '2d35987e-0365-4327-be8c-edd96d187184'  # 180


#
# load useful modules
#
import uuid

#
# create a training run ID
#
uid_training = str(uuid.uuid4())
uid_data = 'blah'

#
# user settings
#
# CRUDE:  Improve on this design... later
#
temp_directory = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning'

config = {

    'uid_data' : uid_data,
    'uid_training' : uid_training,

    'y_forward_set' : 'y_forward_lhc_mean',
    
    'data_source_path' : temp_directory + '/output',
           
    #
    # Do we want to use a variable learning rate?
    # If so, what value?
    #
    'use_variable_learning_rate' : True,
    'learning_rate' : 0.001,

    #
    # Batch normalization
    #
    'use_batch_normalization_layers' : True,
    'batch_normalization_momentum' : 0.9,

    #
    # Dropout
    #
    'use_dropout_layers' : True,
    'dense_dropout_rate' : 0.25,
    'rnn_dropout_rate' : 0.25,
    'rnn_recurrent_dropout_rate' : 0.2,

    #
    # Set seed(s) for repeatability.
    #
    # I'm not sure if the NumPy seed matters given our use
    # of TensorFlow:
    #
    'numpy_seed' : 5, 
    'tensorflow_seed' : 54,

    #
    # Neural network architecture
    #
    'number_of_cells_per_RNN_layer_list' : [200, 200, 200, 200, 200, 200, 100, 100, 100],
    'number_of_cells_per_dense_layer_list' : [50],

    #
    # Activation functions
    #
    'lstm_activation_function' : 'LeakyReLU',
    'dense_activation_function' : 'LeakyReLU',
    'final_dense_activation_function' : 'LeakyReLU',

    'epochs' : 500,
    'batch_size' : 128,

    'loss_function' : 'mse',
    'metrics_to_store' : ['mse'],

    'model_checkpoint_monitor' : 'val_loss',
    'model_checkpoint_save_best_only' : True,

    # 'validation_split' : 0.2,
    
    'regularizer_kernel_L1' : 1e-5,
    'regularizer_kernel_L2' : 1e-4,
    'regularizer_bias_L2' : 1e-4,
    'regularizer_activity_L2' : 1e-5,
    
    'callbacks_dict' : {
        'ReduceLROnPlateau' : {
            'monitor' : 'val_loss',
            'factor' : 0.9,
            'patience' : 4,
        }
    },

    'early_stopping_monitor' : 'loss',
    'early_stopping_patience' : 15,
}

config['json_config_output_path'] = temp_directory + '/output/' + uid_data + '----' + uid_training + '_lstm_regressor_config.json'

config['checkpoint_file_path'] = temp_directory + '/output/' + uid_data + '----' + uid_training + '_regressor_model_checkpoints.keras'
       
config['model_json_path'] = temp_directory + '/output/' + uid_data + '----' + uid_training + '_model_regressor.json'
    
config['model_final_weights_path'] = temp_directory + '/output/' + uid_data + '----' + uid_training + '_final_weights_regressor.pickled'

config['model_final_history_path'] = temp_directory + '/output/' + uid_data + '----' + uid_training + '_final_history_regressor.pickled'
