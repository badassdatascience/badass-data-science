
#
# The following two functions were modified from those detailed at:
#
# https://www.statsmodels.org/dev/examples/notebooks/generated/stationarity_detrending_adf_kpss.html
#

#
# import useful libraries
#
from statsmodels.tsa.stattools import adfuller
from statsmodels.tsa.stattools import kpss

def adf_test(timeseries):
    dftest = adfuller(timeseries, autolag = 'AIC')
    to_return = {
        'test_statistic' : dftest[0],
        'p_value' : dftest[1],
        'n_lags_used' : dftest[2],
        'n_observations_used' : dftest[3],
        'critical_values' : dftest[4],
        'length_of_time_series' : len(timeseries),
    }
    return to_return    

def kpss_test(
    timeseries,
    regression = 'c',
    nlags = 'auto',
):
    kpsstest = kpss(timeseries, regression = regression, nlags = nlags)
    to_return = {
        'test_statistic' : kpsstest[0],
        'p_value' : kpsstest[1],
        'n_lags_used' : kpsstest[2],
    }
    for key, value in kpsstest[3].items():
        to_return['critical_value_' + key.replace(' ', '_')] = value

    return to_return
