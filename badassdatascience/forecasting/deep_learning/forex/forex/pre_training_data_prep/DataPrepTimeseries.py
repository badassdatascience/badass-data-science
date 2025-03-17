import pickle
import numpy as np
import matplotlib.pyplot as plt

class DataPrepTimeseries():

    def __init__(
        self,
        filename_train_val_test_dict,
        config,
        stat_function = None,
    ):

        with open(filename_train_val_test_dict, 'rb') as fff:
            train_val_test_dict = pickle.load(fff)

        self.config = config
        self.features_list = train_val_test_dict['features']['features_list']
        self.features_name_to_index = train_val_test_dict['features']['features_index_lookup']['name_to_index']
        self.features_index_to_name = train_val_test_dict['features']['features_index_lookup']['index_to_name']
        self.matrices = train_val_test_dict['matrices']
        self.timestamps = train_val_test_dict['list_of_timestamps']
        self.stat_function = stat_function
       
        # need to refactor this:
        self.stat_function_to_name_map = {
            np.mean : 'Mean',
            np.median : 'Median',
        }

        # need to refactor this too (maybe):
        if self.stat_function != None:
            self.apply_stat_function_to_y()

    def apply_stat_function_to_y(self):
        y_stat_dict = {}
        for set_name in ['train', 'test', 'val']:
            y_stat_dict[set_name] = (
                self.stat_function(self.matrices[set_name]['y'], axis = 1)
            )
        self.y_stat_dict = y_stat_dict
    
    def get_feature_index_from_feature_name(self, feature_name = 'lhc_mean'):
        return self.features_name_to_index[feature_name]
    
    def get_X_for_a_row(self, set_name = 'train', feature_name = 'lhc_mean', row_index = 0):
        return self.matrices[set_name]['X'][row_index, :, self.get_feature_index_from_feature_name(feature_name = feature_name)]

    def get_y_for_a_row(self, set_name = 'train', feature_name = 'lhc_mean', row_index = 0):
        return self.matrices[set_name]['y'][row_index, :, self.get_feature_index_from_feature_name(feature_name = feature_name)]

    def get_y_summary_statistic_array(self, set_name = 'train', feature_name = 'lhc_mean', function = np.mean):
        y_temp = self.matrices[set_name]['y'][:, :, self.get_feature_index_from_feature_name(feature_name = feature_name)]
        return function(y_temp, axis = 1)
    
    def plot_it(
        self,
        index_panel = 0,
        feature_name = 'lhc_mean',
        summary_statistic_function = np.mean,
        y_predicted = None,
        marker = 'o',
        markersize = 8,
    ):

        static_function_name = self.stat_function_to_name_map[summary_statistic_function]
        
        X_test = self.get_X_for_a_row(set_name = 'test', feature_name = feature_name, row_index = index_panel)
        y_test = self.get_y_for_a_row(set_name = 'test', feature_name = feature_name, row_index = index_panel)

        X_range = np.arange(0, self.config['n_back'])
        y_range = np.arange(self.config['n_back'], (self.config['n_back'] + self.config['n_forward']))

        
        y_summary_statistic_value = self.get_y_summary_statistic_array(set_name = 'test', feature_name = feature_name)[index_panel]


        from matplotlib.colors import TABLEAU_COLORS, same_color
        prop_cycle = plt.rcParams['axes.prop_cycle']
        colors = prop_cycle.by_key()['color']
        
        
        plt.figure()
        plt.plot(X_range, X_test, label = 'Known X', color = colors[0])
        plt.plot(y_range, y_test, label = 'Known Y', color = colors[1])
        
        plt.plot(y_range, [y_summary_statistic_value] * self.config['n_forward'], label = static_function_name + '(Known Y)', color = colors[2])
        if y_predicted != None:
            if len(model_result.y_predicted[0]) == 1:
                y_predicted_value = y_predicted[0]
                plt.plot(y_range, [y_predicted_value] * self.config['n_forward'], label = 'Predicted Mean Y', color = colors[3])
        
        
        
        
        plt.plot([self.config['n_back']], y_test[0], marker = marker, markersize = markersize, label = 'Trade Execution Time', color = 'black')
        
        plt.ylabel(feature_name.replace('_', ' ') + ' (scaled)')
        plt.xlabel('Minutes')
        #plt.xticks(ticks = xticks, labels = labels_xticks)
        
        plt.legend()
        plt.tight_layout()
        plt.show()
        plt.close()
