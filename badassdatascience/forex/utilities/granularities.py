#
# load useful libraries
#
import numpy as np
from scipy import stats
import pandas as pd

#
# function to retrieve the mode of the differenced UNIX epoch timestamps
#
def get_mode_in_diff_seconds(df, column = 'time'):
    diff =  df[column][1:].values - df[column][0:-1].values
    mode = stats.mode(diff)[0]
    return mode

#
# function to produce a range of UNIX epoch timestamps by step
# "get_mode_in_diff_seconds(df, ...)"
#
def range_by_seconds_interval(df, column = 'time'):
    the_min = min(df[column])
    the_max = max(df[column])
    mode = get_mode_in_diff_seconds(df, column = column)
    return np.arange(the_min, the_max + mode, mode)

#
# Retrieve a dataframe having the full UNIX epoch timestamp range
# with nulls where dates are missing in the data pulled from the database
#
# This should produce missing values on the weekend
#
def get_df_merged_by_seconds_interval(df, column = 'time'):
    df_range_by_seconds_interval = pd.DataFrame({column : range_by_seconds_interval(df)})
    return (
        pd.merge(
            df_range_by_seconds_interval, df, on = column, how = 'left',
        )
        .sort_values(by = column)
        .reset_index()
        .drop(columns = 'index')
        .copy()
    )
