import numpy as np
from numpy.lib.stride_tricks import sliding_window_view

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType, IntegerType, BooleanType

from utilities.spark_session import get_spark_session

# https://numpy.org/devdocs/reference/generated/numpy.lib.stride_tricks.sliding_window_view.html


def test_window_space(**config):

    def do_we_have_enough_space_for_a_sliding_window(
        the_list,
        n_back,
        n_forward,
        offset,
    ):
        threshold = n_back + n_forward + offset    
        return threshold <= len(the_list)

    udf_do_we_have_enough_space_for_a_sliding_window = f.udf(do_we_have_enough_space_for_a_sliding_window, BooleanType())


    spark = get_spark_session(config['spark_config'])
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_post_trig'])
    
    sdf_arrays = (
        sdf_arrays
        .withColumn(
            'is_long_enough',
            udf_do_we_have_enough_space_for_a_sliding_window(
                f.col('timestamps_all'),
                f.lit(config['n_back']),
                f.lit(config['n_forward']),
                f.lit(config['offset']),
            )
        )
    )

    sdf_qa = (
        sdf_arrays.select('date_post_shift', 'is_long_enough')
        .groupBy('is_long_enough')
        .agg(f.count('date_post_shift').alias('count_column'))
    )
    sdf_qa.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/QA/' + config['filename_sliding_window_QA'])
    
    sdf_arrays = (
        sdf_arrays
        .where(f.col('is_long_enough'))
        .drop('is_long_enough')
    )
    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_sliding_window_space_check'])
    
    spark.stop()

    
def do_sliding_window(**config):
    spark = get_spark_session(config['spark_config'])
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_sliding_window_space_check'])

    items_list = [('sw_timestamp', 'timestamps_all')]
    items_list.extend([(new_name, old_name) for new_name, old_name in zip([x + '_and_nans' for x in config['list_data_columns']], ['sw_' + x for x in config['list_data_columns']])])
    items_list.extend([(new_name, old_name) for new_name, old_name in zip([x for x in config['list_data_columns_no_scale']], ['sw_' + x for x in config['list_data_columns_no_scale']])])

    def make_sliding_window_float(
        values_array,
        n_back,
        n_forward,
        offset,
        n_step,
    ):
        arr = np.array(values_array)
        if len(arr) <= n_back + n_forward + offset:
            return None  # clean this up
        else:
            swv = sliding_window_view(arr, n_back + n_forward)[::n_step]

        to_return = []
        for i in range(0, swv.shape[0]):
            to_return.append([float(x) for x in swv[i]])

        return to_return

    #def make_sliding_window_int(
    #    values_array,
    #    n_back,
    #    n_forward,
    #    offset,
    #    n_step,
                      #):
    #    arr = np.array(values_array)
    #    if len(arr) <= n_back + n_forward + offset:
    #        return None  # clean this up
    #    else:
    #        swv = sliding_window_view(arr, n_back + n_forward)[::n_step]

    #    to_return = []
    #    for i in range(0, swv.shape[0]):
    #        to_return.append([int(x) for x in swv[i]])
    #
    #    return to_return


    udf_make_sliding_window_float = f.udf(
        make_sliding_window_float,
        ArrayType(ArrayType(FloatType()))
    )


    #udf_make_sliding_window_int = f.udf(
    #    make_sliding_window_int,
    #    ArrayType(ArrayType(IntegerType()))
    #)


                      
    for new_name, old_name in items_list:
        sdf_arrays = (
            sdf_arrays
            .withColumn(
                new_name,
                udf_make_sliding_window_float(
                    f.col(old_name),
                    f.lit(config['n_back']),
                    f.lit(config['n_forward']),
                    f.lit(config['offset']),
                    f.lit(config['n_step']),
                )
            )
            .drop(new_name)
        )

    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_sliding_window'])
    spark.stop()
