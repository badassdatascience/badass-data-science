import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

from forex.pre_training_data_prep.config import config


class ForexDataClass():

    def __init__(self, airflow_run_id, **config):

        # temp hard-coding
        self.sample_index = 0
        self.var_to_forecast = 'lhc_mean'

        
        self.airflow_run_id = airflow_run_id
        self.config = config

        np.random.seed(config['shuffle_random_seed'])

        self.pdf = pd.read_parquet(config['directory_output'] + '/' + self.airflow_run_id + '/' + config['filename_scaled'])
        self.pdf.columns = [x.replace('_scaled', '').replace('_ff', '') for x in self.pdf.columns]


    def fit(self, function = np.mean):

        #
        # percent change in main variable
        #
        self.pdf['X_main_feature'] = [q[0:(self.config['n_back'])] for q in self.pdf[self.var_to_forecast]]
        self.pdf['y_main_feature'] = [q[(self.config['n_back']):] for q in self.pdf[self.var_to_forecast]]
        old = np.array([q[-1] for q in self.pdf['X_main_feature']])
        new = np.array([function(q) for q in self.pdf['y_main_feature']])
        self.pdf['percent_change_to_function'] = ((new - old) / old) * 100.

        self.problem_indices = np.isinf(self.pdf['percent_change_to_function']) | np.isnan(self.pdf['percent_change_to_function'])

        print(self.pdf['timestamp'].values[self.problem_indices])
        
        #print(len(old))
        #print(len(new))

        

    def plot_sample_index(self):
        plot_me_X = self.pdf[['X_main_feature']].iloc[self.sample_index, :]['X_main_feature']
        plot_me_y = self.pdf[['y_main_feature']].iloc[self.sample_index, :]['y_main_feature']

        plt.figure()
        plt.plot(np.arange(0, len(plot_me_X)), plot_me_X)
        plt.plot(np.arange(len(plot_me_X), len(plot_me_X) + config['n_forward']), plot_me_y)
        plt.title('Sample index = ' + str(self.sample_index))
        plt.ylabel(self.var_to_forecast)
        plt.show()
        plt.close()


if __name__ == '__main__':
    fdc = ForexDataClass('manual__2025-03-24T16:50:15.064650+00:00', **config)
    fdc.fit()
