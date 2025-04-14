import numpy
import pandas as pd

def finalize_pandas_candlesticks(**config):

    shifted_as_needed_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_shift_days_and_hours_as_needed']
    pdf = pd.read_parquet(shifted_as_needed_file_name_and_path)

    pdf['Return'] = pdf['c'] - pdf['o']
    pdf['Volatility'] = pdf['h'] - pdf['l']
    pdf['lhc_mean'] = pdf[['l', 'h', 'c']].mean(axis = 1, skipna = True)
        
    pandas_finalized_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_finalized_pandas']
    pdf.to_parquet(pandas_finalized_file_name_and_path)
