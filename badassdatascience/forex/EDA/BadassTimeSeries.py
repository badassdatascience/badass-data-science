import numpy as np
import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt

from badassdatascience.utilities.badass_timeseries import stationarity_and_detrending as sd

from badassdatascience.utilities.badass_timeseries.acf_and_pacf import center_the_ci_around_zero
from badassdatascience.utilities.badass_timeseries.acf_and_pacf import compute_acf_the_way_emily_wants_it
from badassdatascience.utilities.badass_timeseries.acf_and_pacf import compute_pacf_the_way_emily_wants_it
from badassdatascience.utilities.badass_timeseries.acf_and_pacf import plot_acf_and_pacf_the_way_emily_wants_it


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
            .sort_index()
        )
        
        
        self.df_diff_original = (
            pd.DataFrame(
                {
                    'timestamp' : pd.to_datetime(self.df_original.index.values[1:], unit = 's'),
                    'y' : self.df_original['y'].values[1:] - self.df_original['y'].values[0:-1],
                }
            )
            .set_index('timestamp', verify_integrity = True)
            .sort_index()
        )

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
        
        self.df_diff_projected_to_full_timestamp_range = (
            pd.DataFrame(
                {
                    'timestamp' : pd.to_datetime(self.df_projected_to_full_timestamp_range.index.values[1:], unit = 's'),
                    'y' : self.df_projected_to_full_timestamp_range['y'].values[1:] - self.df_projected_to_full_timestamp_range['y'].values[0:-1],
                }
            )
            .set_index('timestamp', verify_integrity = True)
            .sort_index()
        )

    #
    # optionally fill forward (to deal with missing values)
    #
    def fill_forward(self):
        self.df_filled_forward = self.df_projected_to_full_timestamp_range.ffill()
        
        self.df_diff_filled_forward = (
            pd.DataFrame(
                {
                    'timestamp' : pd.to_datetime(self.df_filled_forward.index.values[1:], unit = 's'),
                    'y' : self.df_filled_forward['y'].values[1:] - self.df_filled_forward['y'].values[0:-1],
                }
            )
            .set_index('timestamp', verify_integrity = True)
            .sort_index()
        )

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
    def fit_details(self):
        if self.have_fit:
            self.dict_adf_tests = {
                'original' : sd.adf_test(self.df_original['y']),
                'diff_original' : sd.adf_test(self.df_diff_original['y']),
                'filled_forward' : sd.adf_test(self.df_filled_forward['y']),
                'diff_filled_forward' : sd.adf_test(self.df_diff_filled_forward['y']),
            }
            self.dict_kpss_tests = {
                'original' : sd.kpss_test(self.df_original['y']),
                'diff_original' : sd.kpss_test(self.df_diff_original['y']),
                'filled_forward' : sd.kpss_test(self.df_filled_forward['y']),
                'diff_filled_forward' : sd.kpss_test(self.df_diff_filled_forward['y']),
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


    
    def compute_acf_and_pacf(self, nlags = 200):
        self.acf_original, self.acf_ci_original = compute_acf_the_way_emily_wants_it(self.df_original['y'], nlags = nlags, fft = False)
        self.pacf_original, self.pacf_ci_original = compute_pacf_the_way_emily_wants_it(self.df_original['y'], nlags = nlags)
        self.aCI_original = center_the_ci_around_zero(self.acf_original, self.acf_ci_original)
        self.paCI_original = center_the_ci_around_zero(self.pacf_original, self.pacf_ci_original)
        
        self.acf_diff_original, self.acf_ci_diff_original = compute_acf_the_way_emily_wants_it(self.df_diff_original['y'], nlags = nlags, fft = False)
        self.pacf_diff_original, self.pacf_ci_diff_original = compute_pacf_the_way_emily_wants_it(self.df_diff_original['y'], nlags = nlags)
        self.aCI_diff_original = center_the_ci_around_zero(self.acf_diff_original, self.acf_ci_diff_original)
        self.paCI_diff_original = center_the_ci_around_zero(self.pacf_diff_original, self.pacf_ci_diff_original)

        self.acf_filled_forward, self.acf_ci_filled_forward = compute_acf_the_way_emily_wants_it(self.df_filled_forward['y'], nlags = nlags, fft = False)
        self.pacf_filled_forward, self.pacf_ci_filled_forward = compute_pacf_the_way_emily_wants_it(self.df_filled_forward['y'], nlags = nlags)
        self.aCI_filled_forward = center_the_ci_around_zero(self.acf_filled_forward, self.acf_ci_filled_forward)
        self.paCI_filled_forward = center_the_ci_around_zero(self.pacf_filled_forward, self.pacf_ci_filled_forward)
        
        self.acf_diff_filled_forward, self.acf_ci_diff_filled_forward = compute_acf_the_way_emily_wants_it(self.df_diff_filled_forward['y'], nlags = nlags, fft = False)
        self.pacf_diff_filled_forward, self.pacf_ci_diff_filled_forward = compute_pacf_the_way_emily_wants_it(self.df_diff_filled_forward['y'], nlags = nlags)
        self.aCI_diff_filled_forward = center_the_ci_around_zero(self.acf_diff_filled_forward, self.acf_ci_diff_filled_forward)
        self.paCI_diff_filled_forward = center_the_ci_around_zero(self.pacf_diff_filled_forward, self.pacf_ci_diff_filled_forward)


    def plot_acf_and_pacf(self, first_lag = 1):
        plot_acf_and_pacf_the_way_emily_wants_it(self.acf_original, self.aCI_original, 'ACF Original', first_lag = first_lag, name = self.name)
        plot_acf_and_pacf_the_way_emily_wants_it(self.pacf_original, self.paCI_original, 'PACF Original', first_lag = first_lag, name = self.name)

        plot_acf_and_pacf_the_way_emily_wants_it(self.acf_diff_original, self.aCI_diff_original, 'ACF Diff Original', first_lag = first_lag, name = self.name)
        plot_acf_and_pacf_the_way_emily_wants_it(self.pacf_diff_original, self.paCI_diff_original, 'PACF Diff Original', first_lag = first_lag, name = self.name)

        plot_acf_and_pacf_the_way_emily_wants_it(self.acf_filled_forward, self.aCI_filled_forward, 'ACF Filled Forward', first_lag = first_lag, name = self.name)
        plot_acf_and_pacf_the_way_emily_wants_it(self.pacf_filled_forward, self.paCI_filled_forward, 'PACF Filled Forward', first_lag = first_lag, name = self.name)

        plot_acf_and_pacf_the_way_emily_wants_it(self.acf_diff_filled_forward, self.aCI_diff_filled_forward, 'ACF Diff Filled Forward', first_lag = first_lag, name = self.name)
        plot_acf_and_pacf_the_way_emily_wants_it(self.pacf_diff_filled_forward, self.paCI_diff_filled_forward, 'PACF Diff Filled Forward', first_lag = first_lag, name = self.name)



    #
    # ACF plots
    #
    #def plot_the_acfs(self):
    #    plt.figure()
    #    plot_acf(self.df_original['y'].values)
    #    plt.title('Autocorrelation:  original')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_acf(self.df_diff_original['y'].values)
    #    plt.title('Autocorrelation:  diff_original')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_acf(self.df_diff_original['y'].values)
    #    plt.ylim([-0.025, 0.025])  # hard-coded... ugg... Fix This!
    #    plt.title('Autocorrelation:  diff_original (Zoomed-In)')
    #    plt.show()
    #    plt.close()
    #    
    #    plt.figure()
    #    plot_acf(self.df_filled_forward['y'].values)
    #    plt.title('Autocorrelation:  filled_forward')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_acf(self.df_diff_filled_forward['y'].values)
    #    plt.title('Autocorrelation:  diff_filled_forward')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_acf(self.df_diff_filled_forward['y'].values)
    #    plt.ylim([-0.025, 0.025])  # hard-coded... ugg... Fix This!
    #    plt.title('Autocorrelation:  diff_filled_forward (Zoomed-In)')
    #    plt.show()
    #    plt.close()

    
    #
    # PACF plots
    #
    #def plot_the_pacfs(self):
    #    plt.figure()
    #    plot_pacf(self.df_original['y'].values)
    #    plt.title('Partial Autocorrelation:  original')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_pacf(self.df_diff_original['y'])
    #    plt.title('Partial Autocorrelation:  diff_original')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_pacf(self.df_diff_original['y'])
    #    plt.ylim([-0.025, 0.025])  # hard-coded... ugg... Fix This!
    #    plt.title('Partial Autocorrelation:  diff_original (Zoomed-In)')
    #    plt.show()
    #    plt.close()
    #    
    #    plt.figure()
    #    plot_pacf(self.df_filled_forward['y'].values)
    #    plt.title('Partial Autocorrelation:  filled_forward')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_pacf(self.df_diff_filled_forward['y'])
    #    plt.title('Partial Autocorrelation:  diff_filled_forward')
    #    plt.show()
    #    plt.close()
    #
    #    plt.figure()
    #    plot_acf(self.df_diff_filled_forward['y'])
    #    plt.ylim([-0.025, 0.025])  # hard-coded... ugg... Fix This!
    #    plt.title('Partial Autocorrelation:  diff_filled_forward (Zoomed-In)')
    #    plt.show()
    #    plt.close()

