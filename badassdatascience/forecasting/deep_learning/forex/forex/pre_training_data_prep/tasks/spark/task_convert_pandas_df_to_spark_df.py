from utilities.spark_session import get_spark_session
from utilities.spark_session import load_pandas_df_parquet_into_spark_df

def task_convert_pandas_df_to_spark_df(**config):
    spark = get_spark_session(config['spark_config'])

    sdf_arrays = load_pandas_df_parquet_into_spark_df(
        config['directory_output'] + '/' + config['filename_finalized_pandas'],
        spark,
        truncate_to_row_number = None, #10,
        n_processors_to_coalesce = config['n_processors_to_coalesce'],
    )

    # make the column names lower case
    for item in sdf_arrays.columns:
        sdf_arrays = sdf_arrays.withColumnRenamed(item, item.lower())

    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['filename_conversion_to_spark'])

    spark.stop()
    