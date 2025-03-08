

import pandas as pd
from forex.pre_training_data_prep.config import config

pandas_dataframe_list = [
    config['filename_candlesticks_query_results'],
    config['filename_timezone_added'],
    config['filename_offset'],
    config['filename_weekday_shift_merged'],
]

for item in pandas_dataframe_list:
    print()
    pdf = pd.read_parquet(config['directory_output'] + '/' + item)
    print(pdf)

print()

for item in pandas_dataframe_list:
    pdf = pd.read_parquet(config['directory_output'] + '/' + item)
    print(item, len(pdf.index), len(pdf.dropna().index))

print()

