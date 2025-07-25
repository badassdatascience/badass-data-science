import numpy as np
import matplotlib.pyplot as plt
from statsmodels.tsa.stattools import acf
from statsmodels.tsa.stattools import pacf

def center_the_ci_around_zero(cf, ci):
    M_cf = np.array([cf, cf]).T
    CI = ci - M_cf
    return CI

def compute_acf_the_way_emily_wants_it(x, alpha = 0.05, nlags = 49, fft = True):
    cf, ci = acf(x, alpha = alpha, nlags = nlags, fft = fft)
    return cf, ci

def compute_pacf_the_way_emily_wants_it(x, alpha = 0.05, nlags = 49):
    cf, ci = pacf(x, alpha = alpha, nlags = nlags)
    return cf, ci

def plot_acf_and_pacf_the_way_emily_wants_it(cf, CI, title, first_lag = 0, name = 'mid_c'):
    plt.figure()

    plt.plot(np.arange(first_lag, len(cf)), cf[first_lag:], '.', color = 'blue')
    plt.axhline(0., color = 'black')
    plt.plot(np.arange(first_lag, len(cf)), CI[first_lag:, 0], color = 'orange')
    plt.plot(np.arange(first_lag, len(cf)), CI[first_lag:, 1], color = 'orange')#

    for i, y in zip(np.arange(first_lag, len(cf)), cf[first_lag:]):
        plt.plot([i, i], [0., y], color = 'blue')

    plt.title(title + ' (' + name + ')')
    plt.show()
    plt.close()

def identify_significant_indices(cf, CI):
    indices = (cf <= CI[:, 0]) | (cf >= CI[:, 1])
    return np.arange(0, len(cf))[indices]
