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

    items_list_int = [('timestamps_all', 'sw_timestamp')]
    
    items_list_float = [
        (old_name, new_name) for old_name, new_name in zip(
            [x + '_and_nans' for x in config['list_data_columns']],
            ['sw_' + x for x in config['list_data_columns']]
        )
    ]

    items_list_float.extend(
        [
            (old_name, new_name) for old_name, new_name in zip(
                [x for x in config['list_data_columns_no_scale']],
                ['sw_' + x for x in config['list_data_columns_no_scale']],
            )
        ]
    )
    
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

    def make_sliding_window_int(
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
           to_return.append([int(x) for x in swv[i]])
    
       return to_return


    udf_make_sliding_window_float = f.udf(
        make_sliding_window_float,
        ArrayType(ArrayType(FloatType()))
    )


    udf_make_sliding_window_int = f.udf(
       make_sliding_window_int,
       ArrayType(ArrayType(IntegerType()))
    )


    #
    # Wanted to condense these next two blocks to a single
    # function, but Airflow does not seem to like it:
    #
    for item_tuple in items_list_int:
        old_name = item_tuple[0]
        new_name = item_tuple[1]
        sdf_arrays = (
            sdf_arrays
            .withColumn(
                new_name,
                udf_make_sliding_window_int(
                    f.col(old_name),
                    f.lit(config['n_back']),
                    f.lit(config['n_forward']),
                    f.lit(config['offset']),
                    f.lit(config['n_step']),
                )
            )
            .drop(old_name)
        )
    for item_tuple in items_list_float:
        old_name = item_tuple[0]
        new_name = item_tuple[1]
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
            .drop(old_name)
        )


    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_sliding_window'])
    spark.stop()


def QA_sliding_window_positions(**config):
    import pyspark.sql.functions as f
    from pyspark.sql.types import IntegerType, FloatType
    
    from utilities.spark_session import get_spark_session

    spark = get_spark_session(config['spark_config'])
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_sliding_window'])

    
    def testI(a_list_of_lists):
        i = a_list_of_lists[0][0]
        return i

    def testJ(a_list_of_lists):
        j = a_list_of_lists[20][0]
        return j

    udf_test_int_i = f.udf(testI, IntegerType())
    udf_test_int_j = f.udf(testJ, IntegerType())

    udf_test_float_i = f.udf(testI, FloatType())
    udf_test_float_j = f.udf(testJ, FloatType())

    (
        sdf_arrays
        .withColumn('I_sw_timestamp', udf_test_int_i(f.col('sw_timestamp')))
        .withColumn('J_sw_timestamp', udf_test_int_j(f.col('sw_timestamp')))
        .withColumn('I_sw_return', udf_test_float_i(f.col('sw_return')))
        .withColumn('J_sw_return', udf_test_float_j(f.col('sw_return')))
        .withColumn('I_sw_sin_24', udf_test_float_i(f.col('sw_sin_24')))
        .withColumn('J_sw_sin_24', udf_test_float_j(f.col('sw_sin_24')))
        .select(
            'date_post_shift',
            'I_sw_timestamp', 'J_sw_timestamp',
            'I_sw_return', 'J_sw_return',
            'I_sw_sin_24', 'J_sw_sin_24',
        )
        .write.mode('overwrite').parquet(
            config['directory_output'] + '/' + config['dag_run'].run_id + '/QA/' + config['filename_sliding_window_positions_QA'])
    )

    spark.stop()
