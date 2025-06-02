import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt

from statsmodels.graphics.tsaplots import plot_acf
from statsmodels.graphics.tsaplots import plot_pacf

from badassdatascience.utilities.badass_timeseries import stationarity_and_detrending as sd


class BadassTimeSeries():

    #
    # Constructor
    #
    def __init__(self, timestamps, y, name = 'Time Series'):
        self.name = name
        self.have_fit = False
        self.have_fit_more_details = False

        if str(type(timestamps) == "<class 'pandas.core.series.Series'>"):
            timestamps = timestamps.values

        self.timestamp_difference_mode = (stats.mode(timestamps[1:] - timestamps[0:-1]))[0]
        self.timestamp_min = np.min(timestamps)
        self.timestamp_max = np.max(timestamps)

        self.df_original = (
            pd.DataFrame(
                {
                    'timestamp' : pd.to_datetime(timestamps, unit = 's'),
                    'y' : y,
                }
            )
            .set_index('timestamp', verify_integrity = True)
        )
        self.diff_df_original = self.df_original['y'].values[1:] - self.df_original['y'].values[0:-1]

    #
    # Identify missing values 
    #
    def project_timestamps_to_full_range(self):
        df_full_range_at_granularity = pd.DataFrame(
            {
                'timestamp' : pd.to_datetime(
                    np.arange(self.timestamp_min, self.timestamp_max + self.timestamp_difference_mode, self.timestamp_difference_mode),
                    unit = 's',
                )
            }
        ).set_index('timestamp', verify_integrity = True)
        
        self.df_projected_to_full_timestamp_range = df_full_range_at_granularity.join(self.df_original)
        
        index = pd.DatetimeIndex(
            self.df_projected_to_full_timestamp_range.index,
            freq = str(self.timestamp_difference_mode) + 's',
        )
        
        self.df_projected_to_full_timestamp_range.index = index
        self.diff_df_projected_to_full_timestamp_range = (
            self.df_projected_to_full_timestamp_range['y'].values[1:] - self.df_projected_to_full_timestamp_range['y'].values[0:-1]
        )

    #
    # optionally fill forward (to deal with missing values)
    #
    def fill_forward(self):
        self.df_filled_forward = self.df_projected_to_full_timestamp_range.ffill()
        self.diff_df_filled_forward = self.df_filled_forward['y'].values[1:] - self.df_filled_forward['y'].values[0:-1]

    #
    # basic "fit" operation al la scikit-learn's style
    #
    def fit(self):
        self.project_timestamps_to_full_range()
        self.fill_forward()
        self.qa()
        self.have_fit = True

    #
    # reorganize the stationarity test information into a table
    #
    # https://www.statsmodels.org/dev/examples/notebooks/generated/stationarity_detrending_adf_kpss.html
    #
    def reorganize_stationarity_test_information(self):
        if self.have_fit_more_details:
            item_list = []

            for test_name, the_dict in zip(['ADF', 'KPSS'], [self.dict_adf_tests, self.dict_kpss_tests]):
                for key in the_dict.keys():
                    result_dict = {
                        'DataFrame' : key,
                        'test' : test_name,
                        'p_value' : the_dict[key]['p_value'],
                    }

                    if the_dict[key]['p_value'] <= 0.05:  # hard-coded... FIX!
                        result_dict['decision'] = 'Reject the null'
                    elif the_dict[key]['p_value'] > 0.05:  # hard-coded... FIX!
                        result_dict['decision'] = 'Fail to reject the null'
                    else:
                        # this shouldn't be possible
                        pass  # do something here

                    if test_name == 'ADF':
                        if result_dict['decision'] == 'Reject the null':
                            result_dict['conclusion'] = 'Evidence suggests that the series is stationary'
                        else:
                            result_dict['conclusion'] = 'Evidence suggests that the series is non-stationary'
                    elif test_name == 'KPSS':
                        if result_dict['decision'] == 'Reject the null':
                            result_dict['conclusion'] = 'Evidence suggests that the series is non-stationary'
                        else:
                            result_dict['conclusion'] = 'Evidence suggests that the series is trend stationary'
                    else:
                        pass  # do something here 
                    
                    item_list.append(result_dict)
            self.df_stationarity_test_table = pd.DataFrame(item_list)
        else:
            print('Need to run "fit_more_details" first.')
    
    #
    # optionally add more information to the "fit"
    #
    def fit_more_details(self):
        if self.have_fit:
            self.dict_adf_tests = {
                'original' : sd.adf_test(self.df_original['y']),
                'diff_original' : sd.adf_test(self.diff_df_original),
                'filled_forward' : sd.adf_test(self.df_filled_forward['y']),
                'diff_filled_forward' : sd.adf_test(self.diff_df_filled_forward),
            }
            self.dict_kpss_tests = {
                'original' : sd.kpss_test(self.df_original['y']),
                'diff_original' : sd.kpss_test(self.diff_df_original),
                'filled_forward' : sd.kpss_test(self.df_filled_forward['y']),
                'diff_filled_forward' : sd.kpss_test(self.diff_df_filled_forward),
            }
        else:
            print('Need to run "fit" method before running "fit_more_details" method.')

        self.have_fit_more_details = True
        self.reorganize_stationarity_test_information()

    #
    # QA
    #
    def qa(self):
        assert(len(self.df_original) == len(self.df_original.dropna()))
        assert(len(self.df_projected_to_full_timestamp_range) >= len(self.df_projected_to_full_timestamp_range.dropna()))
        assert(len(self.df_filled_forward) == len(self.df_filled_forward.dropna()))

    #
    # ACF plots
    #
    def plot_the_acfs(self):
        plt.figure()
        plot_acf(self.df_original['y'].values)
        plt.title('Autocorrelation:  original')
        plt.show()
        plt.close()

        plt.figure()
        plot_acf(self.diff_df_original)
        plt.title('Autocorrelation:  diff_original')
        plt.show()
        plt.close()

        plt.figure()
        plot_acf(self.df_filled_forward['y'].values)
        plt.title('Autocorrelation:  filled_forward')
        plt.show()
        plt.close()

        plt.figure()
        plot_acf(self.diff_df_filled_forward)
        plt.title('Autocorrelation:  diff_filled_forward')
        plt.show()
        plt.close()

    #
    # PACF plots
    #
    def plot_the_pacfs(self):
        plt.figure()
        plot_pacf(self.df_original['y'].values)
        plt.title('Partial Autocorrelation:  original')
        plt.show()
        plt.close()

        plt.figure()
        plot_pacf(self.diff_df_original)
        plt.title('Partial Autocorrelation:  diff_original')
        plt.show()
        plt.close()

        plt.figure()
        plot_pacf(self.df_filled_forward['y'].values)
        plt.title('Partial Autocorrelation:  filled_forward')
        plt.show()
        plt.close()

        plt.figure()
        plot_pacf(self.diff_df_filled_forward)
        plt.title('Partial Autocorrelation:  diff_filled_forward')
        plt.show()
        plt.close()