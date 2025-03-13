import numpy as np
import pandas as pd

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, FloatType, BooleanType

from utilities.spark_session import get_spark_session



def get_all_timestamps(timestamp_array, seconds_divisor):
    return [int(x) for x in range(min(timestamp_array), max(timestamp_array) + seconds_divisor, seconds_divisor)]

udf_get_all_timestamps = f.udf(get_all_timestamps, ArrayType(IntegerType()))

##https://stackoverflow.com/questions/41190852/most-efficient-way-to-forward-fill-nan-values-in-numpy-array
#def do_nans_exist(values_array):
#    values_array = np.array([np.array(values_array)])
#    mask = np.isnan(values_array)
#    has_nan_0_or_1 = np.max([int(x) for x in mask[0]])
#    return int(has_nan_0_or_1)
#
#udf_do_nans_exist = f.udf(do_nans_exist, IntegerType())

##https://stackoverflow.com/questions/41190852/most-efficient-way-to-forward-fill-nan-values-in-numpy-array
#def do_non_nans_exist(values_array):
#    values_array = np.array([np.array(values_array)])
#    mask = ~np.isnan(values_array)
#    has_non_nan_0_or_1 = np.max([int(x) for x in mask[0]])
#    return int(has_non_nan_0_or_1)
#    
#udf_do_non_nans_exist = f.udf(do_non_nans_exist, IntegerType())


def count_nans_in_array(values_array):
    values_array = np.array([np.array(values_array)])
    mask = np.isnan(values_array)
    nan_count = np.sum([int(x) for x in mask[0]])
    return int(nan_count)
    
udf_count_nans_in_array = f.udf(count_nans_in_array, IntegerType())



##https://stackoverflow.com/questions/41190852/most-efficient-way-to-forward-fill-nan-values-in-numpy-array
#def forward_fill(values_array):
#    arr = np.array([values_array])
#    mask = np.isnan(arr)
#    idx = np.where(~mask, np.arange(mask.shape[1]), 0)
#    np.maximum.accumulate(idx, axis = 1, out = idx)
#    arr[mask] = arr[np.nonzero(mask)[0], idx[mask]]
#    to_return = list([float(x) for x in arr[0]])
#    return to_return
#
#udf_forward_fill = f.udf(forward_fill, ArrayType(FloatType()))


def locate_nans(timestamp_array, timestamp_all_array, values_array):

    # make sure we get an argsort in here later to ensure order of values is correct

    ts = np.array(timestamp_array, dtype = np.uint64) # ??
    ts_all = np.array(timestamp_all_array, dtype = np.uint64)  # we can probably make this smaller
    v = np.array(values_array, dtype = np.float64)  # we can probably make this smaller
    
    pdf = pd.DataFrame({'timestamp' : ts, 'values' : v})
    pdf_all = pd.DataFrame({'timestamp' : ts_all})

    pdf_joined = (
        pd.merge(
            pdf_all,
            pdf,
            on = 'timestamp',
            how = 'left',
        )
    )

    to_return = pdf_joined['values'].to_list()
    
    return to_return

udf_locate_nans = f.udf(locate_nans, ArrayType(FloatType()))



def task_find_full_day_nans(**config):

    spark = get_spark_session(config['spark_config'])
    sdf_arrays = (
        spark.read.parquet(config['directory_output'] + '/' + config['filename_timestamp_diff'])
        .coalesce(config['n_processors_to_coalesce'])
        .orderBy('date_post_shift')
        .withColumn(
            'timestamps_all',
            udf_get_all_timestamps(f.col('sorted_timestamp_array'), f.lit(config['seconds_divisor']))
        )
    )
    
    for item in config['list_data_columns']:
        sdf_arrays = (
            sdf_arrays
            .withColumn(
                item + '_and_nans',
                udf_locate_nans(f.col('sorted_timestamp_array'), f.col('timestamps_all'), f.col('sorted_' + item + '_array'))
            )
        )

    for item in config['list_data_columns']:
        sdf_arrays = (
            sdf_arrays
            .withColumn(
                item + '_nan_count',
                udf_count_nans_in_array(f.col(item + '_and_nans'))
            )
        )

    sdf_arrays = sdf_arrays.withColumn('nan_count_full_day', f.col('return_nan_count'))
    for item in config['list_data_columns']:
        sdf_arrays = sdf_arrays.drop(item + '_nan_count')

    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['filename_full_day_nans'])
    spark.stop()





def get_max_consecutive_NaNs(a_list):

    n_consec_nan_list = [0]
    count = 0
    is_in_nan_group = False
    for item in np.array(a_list):
        if np.isnan(item) or item == None:
            is_in_nan_group = True
            count += 1
        if not np.isnan(item) and is_in_nan_group:
            is_in_nan_group = False
            n_consec_nan_list.append(count)
            count = 0
        
    return max(n_consec_nan_list)
    
udf_get_max_consecutive_NaNs = f.udf(get_max_consecutive_NaNs, IntegerType())