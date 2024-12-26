#
# load useful modules
#
import numpy as np
import pandas as pd

#
# define a function to compare two model fits' AIC and BIC
# and report it in a table
#
def nice_model_fit_comparison_and_report(fit_1, fit_2):
    M = np.array(
        [
            [fit_1.aic, fit_2.aic],
            [fit_1.bic, fit_2.bic],
        ]
    )

    df_fit_report = pd.DataFrame(M, columns = ['Fit #1', 'Fit #2'])
    df_fit_report.index = ['AIC', 'BIC']
    
    return df_fit_report