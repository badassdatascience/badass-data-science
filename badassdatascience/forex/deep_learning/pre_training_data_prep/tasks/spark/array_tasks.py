import numpy as np

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, FloatType

from utilities.spark_session import get_spark_session



def task_pivot_and_sort_arrays(**config):

    def argsort_an_array(the_array):
        the_sort_order = np.argsort(np.array(the_array))
        return [int(x) for x in the_sort_order]

    udf_argsort_an_array = f.udf(argsort_an_array, ArrayType(IntegerType()))

    def apply_argsort_integer(the_array, argsort_array):
        return [int(x) for x in np.array(the_array)[argsort_array]]

    def apply_argsort_float(the_array, argsort_array):
        return [float(x) for x in np.array(the_array)[argsort_array]]

    udf_apply_argsort_integer = f.udf(apply_argsort_integer, ArrayType(IntegerType()))
    udf_apply_argsort_float = f.udf(apply_argsort_float, ArrayType(FloatType()))

    
    spark = get_spark_session(config['spark_config'])

    items_selected_pre_pivot = ['original_date_shifted', 'timestamp']
    items_selected_pre_pivot.extend(config['list_data_columns'])

    sdf_arrays = (
        spark.read.parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_conversion_to_spark'])
        .coalesce(config['n_processors_to_coalesce'])
        .select(*items_selected_pre_pivot)
        .withColumnRenamed('original_date_shifted', 'date_post_shift')
        
    )
    
    assembled_agg_to_eval = """sdf_arrays.orderBy('timestamp').groupBy('date_post_shift').agg(f.collect_list('timestamp').alias('timestamp_array'), """
    for item in config['list_data_columns']:
        assembled_agg_to_eval += """f.collect_list('""" + item + """').alias('""" + item + """_array'), """
    assembled_agg_to_eval = assembled_agg_to_eval.strip() + ')'
    sdf_arrays = eval(assembled_agg_to_eval).orderBy('date_post_shift').coalesce(config['n_processors_to_coalesce'])

    
    # ensure proper sorting (probably not necessary but insurance)
    sdf_arrays = (
        sdf_arrays
        
        
        .withColumn(
            'timestamp_argsort',
            udf_argsort_an_array(f.col('timestamp_array'))
        )
        .withColumn('sorted_timestamp_array', udf_apply_argsort_integer(f.col('timestamp_array'), f.col('timestamp_argsort')))
        .drop('timestamp_array')
    )
    for item in config['list_data_columns']:
        sdf_arrays = (
            sdf_arrays
            .withColumn('sorted_' + item + '_array', udf_apply_argsort_float(f.col(item + '_array'), f.col('timestamp_argsort')))
            .drop(item + '_array')
        )
            
    sdf_arrays = (
        sdf_arrays
        .drop('timestamp_argsort')
        .orderBy('date_post_shift')
    )
    
    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_pivot_and_sort'])
    spark.stop()


def task_diff_the_timestamp_arrays(**config):

    def difference_an_array(the_array, seconds_divisor):
        return [int((y - x) / seconds_divisor) for x, y in zip(the_array[0:-1], the_array[1:])]

    udf_difference_an_array = f.udf(difference_an_array, ArrayType(IntegerType()))

    spark = get_spark_session(config['spark_config'])
    sdf_arrays = (
        spark.read.parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_pivot_and_sort'])
        .withColumn('seconds_divisor', f.lit(config['seconds_divisor']))
        .withColumn(
            'diff_sorted_timestamp_array',
            udf_difference_an_array(
                f.col('sorted_timestamp_array'),
                f.col('seconds_divisor'),
            )
        )
        .drop('seconds_divisor')
        .orderBy('date_post_shift')
    )
    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_timestamp_diff'])
    spark.stop()
