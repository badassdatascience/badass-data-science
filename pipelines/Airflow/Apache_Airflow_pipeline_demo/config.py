
config = {
    'database_name' : 'django',

    'list_data_columns' : ['return', 'volatility', 'volume', 'lhc_mean'],
    'list_data_columns_no_scale' : ['sin_24', 'cos_24'],

    'n_back' : 180,
    'n_forward' : 30,
    'offset' : 1,
    'n_step' : 10,   # investigate this

    'y_feature_to_predict' : 'lhc_mean',

    'use_n_step_in_matrix_prep' : True,
    'n_step_in_matrix_prep' : 5,  # and investigate this
    
    'tz_name' : 'US/Eastern',  # DON'T change this!
    
    'price_type_name' : 'mid',
    'instrument_name' : 'EUR/USD',
    'interval_name' : 'Minute',

    'retries_pull_forex_data' : 1,
    'retry_delay_minutes_pull_forex_data' : 1,

    'cutoff_max_consec_nans' : 2,
    'cutoff_total_nan_count' : 5,

    'shuffle_random_seed' : 42,
    'train_val_test_split' : [0.7, 0.15, 0.15],
    'shuffle_X_train_and_val' : True,


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
    'filename_qa_day_has_nans' : 'spark_qa_day_has_nans.parquet',
    'filename_qa_day_nan_counts' : 'spark_qa_day_nan_counts.parquet',
    'filename_qa_full_day_boxplot' : 'image_qa_full_day_boxplot.png',
    'filename_qa_full_day_boxplot_per_day' : 'image_qa_full_day_boxplot_per_day.png',
    'filename_qa_full_day_consecutive_nans' : 'spark_qa_full_day_consecutive_nans.parquet',
    
    'filename_post_trig' : 'spark_post_trig.parquet',

    'filename_sliding_window_space_check' : 'spark_sliding_window_space_check.parquet',
    'filename_sliding_window_QA' : 'spark_sliding_window_QA.parquet',
    'filename_sliding_window' : 'spark_sliding_window.parquet',
    'filename_sliding_window_positions_QA' : 'spark_sliding_window_positions_QA.parquet',

    'filename_explode_array' : 'spark_explode_array.parquet',
    'filename_post_sw_nans' : 'spark_post_sw_nans.parquet',
    'filename_post_nan_filters' : 'spark_post_nan_filters.parquet',
    'filename_post_sw_plot' : 'post_sliding_window_nans.png',
    'filename_scaling_stats' : 'spark_scaling_stats.parquet',
    'filename_forward_filled' : 'spark_forward_filled',
    'filename_scaled' : 'spark_scaled',
    'filename_final_pandas_df' : 'pandas_final_df.parquet',
    'filename_numpy_final_dict' : 'dict_final_numpy.pickled',

    
    'spark_config' : [
        ('spark.executor.memory', '100g'),
        ('spark.executor.cores', '20'),
        ('spark.cores.max', '20'),
        ('spark.driver.memory', '100g'),
        ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
    ],

    'n_processors_to_coalesce' : 20,
    'seconds_divisor' : 60,
}
