import pyspark.sql.functions as f

from utilities.spark_session import get_spark_session

def task_expand_arrays(**config):

    spark = get_spark_session(config['spark_config'])
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + config['filename_sliding_window'])

    list_to_arrays_zip = ['sw_timestamp']
    list_to_arrays_zip.extend(['sw_' + x for x in config['list_data_columns']])
    list_to_arrays_zip.extend(['sw_' + x for x in config['list_data_columns_no_scale']])

    list_to_select = [f.col('date_post_shift'), f.col('zipped_array.sw_timestamp').alias('timestamp')]

    for item in config['list_data_columns']:
        name = 'zipped_array.sw_' + item
        list_to_select.append(f.col(name).alias(item))
    for item in config['list_data_columns_no_scale']:
        name = 'zipped_array.sw_' + item
        list_to_select.append(f.col(name).alias(item))

    sdf_arrays = (
        sdf_arrays
        .withColumn('zipped_array', f.arrays_zip(*list_to_arrays_zip))
        .withColumn('zipped_array', f.explode('zipped_array'))
        .select(*list_to_select)

        #
        # sort by first timestamp
        #
        .withColumn('timestamp', f.col('timestamp').getItem(0))
        .orderBy('timestamp')
    )

    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['filename_explode_array'])

    spark.stop()