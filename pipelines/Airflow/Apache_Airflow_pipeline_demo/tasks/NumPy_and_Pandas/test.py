import pandas as pd
import numpy as np
import pickle

from forex.pre_training_data_prep.config import config


def final_RNN_prep(**config):

    #run_id = config['dag_run'].run_id
    run_id = 'manual__2025-03-24T16:50:15.064650+00:00'
    
    np.random.seed(config['shuffle_random_seed'])

    dict_results = {}
    
    pdf = pd.read_parquet(config['directory_output'] + '/' + run_id + '/' + config['filename_scaled'])
    pdf.columns = [x.replace('_scaled', '').replace('_ff', '') for x in pdf.columns]

    print()
    print(pdf)
    print()
    print(pdf.columns)
    print()
    import sys; sys.exit(0)
    
    item_list = []
    item_list.extend(config['list_data_columns'])
    item_list.extend(config['list_data_columns_no_scale'])

    dict_results['features'] = {
        'features_list' : item_list,
        'features_index_lookup' : {
            'name_to_index' : {},
            'index_to_name' : {},
        },
    }
    for i, item in enumerate(item_list):
        dict_results['features']['features_index_lookup']['index_to_name'][i] = item
        dict_results['features']['features_index_lookup']['name_to_index'][item] = i

    # ensure we have the columns completely named how we want them
    new_column_names = ['date_post_shift', 'timestamp']
    new_column_names.extend(item_list)
    new_column_names.extend(['mean_' + item for item in item_list if item in config['list_data_columns']])
    new_column_names.extend(['std_' + item for item in item_list if item in config['list_data_columns']])

    pdf = pdf[new_column_names]
    pdf.sort_values(by = ['timestamp'], inplace = True)  # I probably did this earlier but don't want to fuss with the matter right now

    dict_results['list_of_timestamps'] = [int(x) for x in pdf['timestamp'].values]
    dict_results['list_of_shifted_dates'] = [str(x) for x in pdf['date_post_shift'].values]

    n_back = config['n_back']
    n_forward = config['n_forward']

    M_array = pdf[item_list].to_numpy()
    n_rows, n_features = M_array.shape
    
    # I'm sure there is a more elegant way to do this--without an explicitly programmed loop, that is
    #
    M_Xy = np.empty([n_rows, n_back + n_forward, n_features])
    for row in range(0, n_rows):
        for feature_track in range(0, n_features):
            M_Xy[row, :, feature_track] = M_array[row, feature_track]
    
    # see if we use n_step any where else, as its use here might not be correct
    if config['use_n_step_in_matrix_prep']:
        M_Xy = M_Xy[::config['n_step_in_matrix_prep'], :, :]   

    # this assumes we want chronological order
    indices_train = np.uint64(np.arange(0, np.uint64(np.floor(np.round(M_Xy.shape[0] * config['train_val_test_split'][0])))))
    indices_val = np.uint64(np.arange(indices_train[-1] + 1, np.uint64(np.floor(np.round(indices_train[-1] + M_Xy.shape[0] * config['train_val_test_split'][1])))))
    indices_test = np.uint64(np.arange(indices_val[-1] + 1, np.uint64(np.floor(np.round(indices_val[-1] + M_Xy.shape[0] * config['train_val_test_split'][2])))))


    if config['shuffle_X_train_and_val']:
        np.random.shuffle(indices_train)
        np.random.shuffle(indices_val)
        np.random.shuffle(indices_test)

    M_Xy_train = M_Xy[indices_train, :, :]
    M_Xy_val = M_Xy[indices_val, :, :]
    M_Xy_test = M_Xy[indices_test, :, :]

    #
    # the following could have been condensed into one for loop, but I decided for ease of debugging to code this operation very specifically
    #
    dict_results['matrices'] = {'train' : {}, 'val' : {}, 'test' : {}}
    dict_results['matrices']['train'] = {
        'X' : M_Xy_train[:, 0:n_back, :],
        'y' : M_Xy_train[:, n_back:(n_back + n_forward), :],
    }
    dict_results['matrices']['val'] = {
        'X' : M_Xy_val[:, 0:n_back, :],
        'y' : M_Xy_val[:, n_back:(n_back + n_forward), :],
    }
    dict_results['matrices']['test'] = {
        'X' : M_Xy_test[:, 0:n_back, :],
        'y' : M_Xy_test[:, n_back:(n_back + n_forward), :],
    }

    index_y_feature = dict_results['features']['features_index_lookup']['name_to_index'][config['y_feature_to_predict']]

    dict_y_forward = {}
    for item in ['train', 'val', 'test']:
        dict_y_forward[item] = dict_results['matrices'][item]['y'][:, :, index_y_feature]

    dict_y_stats = {}
    for item in dict_y_forward.keys():
        dict_y_stats[item] = {
            'mean' : np.expand_dims(np.mean(dict_y_forward[item], axis = 1), axis = 1),
            'min' : np.expand_dims(np.min(dict_y_forward[item], axis = 1), axis = 1),
            '25th_percentile' : np.expand_dims(np.percentile(dict_y_forward[item], 25, axis = 1), axis = 1),
            'median' : np.expand_dims(np.median(dict_y_forward[item], axis = 1), axis = 1),
            '75th_percentile' : np.expand_dims(np.percentile(dict_y_forward[item], 75, axis = 1), axis = 1),
            'max' : np.expand_dims(np.max(dict_y_forward[item], axis = 1), axis = 1),
        }

    dict_results['y_stats'] = dict_y_stats
    
    #
    # trinary y split, for classification
    #
    # this should probably be computed before the reduction in rows
    #
    trinary_classes = {}
    for item in dict_y_stats.keys():
        old = np.expand_dims(dict_results['matrices'][item]['X'][:, -1, index_y_feature], axis = 1)
        new = dict_y_stats[item]['mean']
        percent_change = ((new - old) / old) * 100.
        dict_y_stats[item]['percent_change_to_mean'] = percent_change

        y_divide_by_zero_mask = old != 0.
        y = np.expand_dims(percent_change[y_divide_by_zero_mask], axis = 1)
        
        trinary_classes[item] = {
            'y_divide_by_zero_mask' : y_divide_by_zero_mask,
            'y' : y,
        }

    lower_tercile_cutoff = np.percentile(dict_y_stats['train']['percent_change_to_mean'], 100./3., axis = 0)[0]
    upper_tercile_cutoff = np.percentile(dict_y_stats['train']['percent_change_to_mean'], 200./3., axis = 0)[0]

    for item in dict_y_stats.keys():
        q = dict_y_stats[item]['percent_change_to_mean'] < lower_tercile_cutoff # short
        r = (lower_tercile_cutoff <= dict_y_stats[item]['percent_change_to_mean']) & (dict_y_stats[item]['percent_change_to_mean'] <= upper_tercile_cutoff) # do nothing
        s = upper_tercile_cutoff < dict_y_stats[item]['percent_change_to_mean'] # buy
        
        M = np.zeros([q.shape[0], 3])
        M[:, 0] = q[:, 0] # short
        M[:, 1] = r[:, 0] # no nothing
        M[:, 2] = s[:, 0] # buy

        trinary_classes[item]['one_hot_encoding'] = M[ trinary_classes[item]['y_divide_by_zero_mask'][:, 0] ]
        trinary_classes[item]['X'] = dict_results['matrices'][item]['X'][ trinary_classes[item]['y_divide_by_zero_mask'][:, 0] ]
        
    dict_results['trinary_classes'] = trinary_classes
    
    #
    # save
    #
    with open(config['directory_output'] + '/' + run_id + '/' + config['filename_numpy_final_dict'], 'wb') as fff:
        pickle.dump(dict_results, fff)
    
    ### save the reorganized pandas dataframe
    # accidently deleted this line. Fix it.


if __name__ == '__main__':
    final_RNN_prep(**config)
