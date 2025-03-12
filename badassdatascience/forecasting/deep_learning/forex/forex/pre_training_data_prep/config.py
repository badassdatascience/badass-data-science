
config = {
    'database_name' : 'django',
    'dag_id' : 'NEW_prepare_forex_data',

    'tz_name' : 'US/Eastern',  # DON'T change this!
    
    'price_type_name' : 'mid',
    'instrument_name' : 'EUR/USD',
    'interval_name' : 'Minute',

    'retries_pull_forex_data' : 1,
    'retry_delay_minutes_pull_forex_data' : 5,

    'directory_output' : '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/forex/output',
    'filename_candlesticks_query_results' : 'candlesticks_query_results.parquet',
    'filename_timezone_added' : 'candlesticks_timezone_added.parquet',
    'filename_offset' : 'candlesticks_timezone_weekday_offset.parquet',
    'filename_weekday_shift_merged' : 'candlesticks_weekday_offset_merged.parquet',
    'filename_shift_days_and_hours_as_needed' : 'candlesticks_shifted_as_needed.parquet',
    'filename_finalized_pandas' : 'candlesticks_finalized_pandas.parquet',
    'filename_conversion_to_spark' : 'spark_converted.parquet',
    'filename_pivot_and_sort' : 'spark_pivot_and_sort.parquet',
    'filename_timestamp_diff' : 'spark_timestamp_diff.parquet',
    'filename_full_day_nans' : 'spark_full_day_nans.parquet',

    'spark_config' : [
        ('spark.executor.memory', '75g'),
        ('spark.executor.cores', '20'),
        ('spark.cores.max', '20'),
        ('spark.driver.memory', '75g'),
        ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
    ],

    'n_processors_to_coalesce' : 20,
    'seconds_divisor' : 60,
}
