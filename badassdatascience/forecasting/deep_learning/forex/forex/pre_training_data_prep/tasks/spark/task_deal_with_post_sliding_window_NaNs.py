
def deal_with_post_sliding_window_nans(**config):

    import numpy as np
    
    import pyspark.sql.functions as f
    from pyspark.sql.types import IntegerType
    
    from utilities.spark_session import get_spark_session

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

    def count_nans_in_array(values_array):
       values_array = np.array([np.array(values_array)])
       mask = np.isnan(values_array)
       nan_count = np.sum([int(x) for x in mask[0]])
       return int(nan_count)
    
    udf_count_nans_in_array = f.udf(count_nans_in_array, IntegerType())

    
    spark = get_spark_session(config['spark_config'])
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_explode_array'])

    items_to_check_list = config['list_data_columns'].copy()
    items_to_check_list.extend(config['list_data_columns_no_scale'])
    
    for item in items_to_check_list:
        sdf_arrays = sdf_arrays.withColumn('max_consec_nan_' + item, udf_get_max_consecutive_NaNs(f.col(item)))

    # We are skipping the QA equality test(s) here. Add it later.

    # is first item NaN?
    # drop if yes
    sdf_arrays = (
        sdf_arrays
        .withColumn('is_first_item_a_nan', f.isnan(f.col('return').getItem(0)))
        .where(~f.col('is_first_item_a_nan'))
        .drop('is_first_item_a_nan')
    )

    # max consecutive NaN count
    # decide on a threshold value after plotting the distribution
    sdf_arrays = (
        sdf_arrays
        .withColumn('max_consec_nans', f.col('max_consec_nan_return'))
        .drop(*['max_consec_nan_' + x for x in items_to_check_list])
    )

    # Again, we are skipping the QA equality test(s) here. Add it later.
    for item in items_to_check_list:
        sdf_arrays = (
            sdf_arrays
            .withColumn(
                item + '_total_nan_count',
                udf_count_nans_in_array(f.col(item))
            )
        )
    sdf_arrays = (
        sdf_arrays
        .withColumn('total_nan_count', f.col(config['list_data_columns'][0] + '_total_nan_count'))
        .drop(*[x + '_total_nan_count' for x in items_to_check_list])
    )

    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_post_sw_nans'])    
    spark.stop()
