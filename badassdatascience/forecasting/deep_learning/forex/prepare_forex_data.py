

# Apache Airflow libraries
from airflow import DAG
from airflow.operators.python import PythonOperator

# common system libraries
from datetime import datetime, timedelta

# local libraries
from forex.pre_training_data_prep.config import config


#
# Define initialization function
#
# This is the first task run below:
#
def prepare_DAG_run(**config):
    import os
    run_directory_output = config['directory_output'] + '/' + config['dag_run'].run_id
    os.system('mkdir ' + run_directory_output)
    os.system('mkdir ' + run_directory_output + '/QA')


###########
#   DAG   #
###########

with DAG(
        dag_id = 'prepare_forex_data',
        start_date = datetime(2024, 1, 1),    # change this at some point
        schedule_interval = None,
        catchup = False,
) as dag:

    #
    # specify common kwargs (config is frozen at this point)
    #
    common_kwargs = {
        'op_kwargs' : config,
        'retries' : config['retries_pull_forex_data'],
        'retry_delay' : timedelta(
            minutes = config['retry_delay_minutes_pull_forex_data'],
        ),
        'provide_context' : True,
    }

    
    #####################################
    #   Define initialization task(s)   #
    #####################################
    
    task_prepare_DAG_run = PythonOperator(
        task_id = 'task_prepare_DAG_run',
        python_callable = prepare_DAG_run,
        **common_kwargs,
    )

    
    #################################
    #   Define Pandas-based tasks   #
    #################################

    #
    # Pull candlestick data from the database
    #
    from forex.pre_training_data_prep.pull_forex_data import pull_forex_data
    task_pull_forex_data = PythonOperator(
        task_id = 'task_pull_forex_data',
        python_callable = pull_forex_data,
        **common_kwargs,
    )

    #
    # Add timezone information
    #
    from forex.pre_training_data_prep.add_timezone_information import add_timezone_information
    task_add_timezone_information = PythonOperator(
        task_id = 'task_add_timezone_information',
        python_callable = add_timezone_information,
        **common_kwargs,
    )

    #
    # Create weekday offset map
    #
    from forex.pre_training_data_prep.offset import generate_offset_map
    task_generate_offset_map = PythonOperator(
        task_id = 'task_generate_offset_map',
        python_callable = generate_offset_map,
        **common_kwargs,
    )

    #
    # merge candlesticks and weekday offset data
    #
    from forex.pre_training_data_prep.offset import merge_offset_map
    task_merge_offset_map = PythonOperator(
        task_id = 'task_merge_offset_map',
        python_callable = merge_offset_map,
        **common_kwargs,
    )

    #
    # perform the final date shift
    #
    from forex.pre_training_data_prep.offset import shift_days_and_hours_as_needed
    task_shift_days_and_hours_as_needed = PythonOperator(
        task_id = 'task_shift_days_and_hours_as_needed',
        python_callable = shift_days_and_hours_as_needed,
        **common_kwargs,
    )

    #
    # finalize the pandas portion
    #
    from forex.pre_training_data_prep.finalize_pandas_df import finalize_pandas_candlesticks
    task_finalize_pandas_candlesticks = PythonOperator(
        task_id = 'task_finalize_pandas_candlesticks',
        python_callable = finalize_pandas_candlesticks,
        **common_kwargs,
    )

    
    #####################
    #   Spark portion   #
    #####################

    from forex.pre_training_data_prep.tasks.spark.task_convert_pandas_df_to_spark_df import task_convert_pandas_df_to_spark_df
    task_spark_convert_pandas_df_to_spark_df = PythonOperator(
        task_id = 'task_spark_convert_pandas_df_to_spark_df',
        python_callable = task_convert_pandas_df_to_spark_df,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.array_tasks import task_pivot_and_sort_arrays
    task_spark_pivot_and_sort_arrays = PythonOperator(
        task_id = 'task_spark_pivot_and_sort_arrays',
        python_callable = task_pivot_and_sort_arrays,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.array_tasks import task_diff_the_timestamp_arrays
    task_spark_diff_the_timestamp_arrays = PythonOperator(
        task_id = 'task_spark_diff_the_timestamp_arrays',
        python_callable = task_diff_the_timestamp_arrays,
        **common_kwargs,
    )
        
    from forex.pre_training_data_prep.tasks.spark.nan_related_tasks import task_find_full_day_nans
    task_spark_find_full_day_nans = PythonOperator(
        task_id = 'task_spark_find_full_day_nans',
        python_callable = task_find_full_day_nans,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.QA.qa_full_day import qa_full_day_nans
    task_QA_full_day_nans = PythonOperator(
        task_id = 'task_QA_full_day_nans',
        python_callable = qa_full_day_nans,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.QA.qa_full_day import qa_full_day_consecutive_nans
    task_QA_full_day_consecutive_nans = PythonOperator(
        task_id = 'task_QA_full_day_consecutive_nans',
        python_callable = qa_full_day_consecutive_nans,
        **common_kwargs,
    )
    
    from forex.pre_training_data_prep.tasks.spark.trig import add_trig
    task_spark_add_trig = PythonOperator(
        task_id = 'task_spark_add_trig',
        python_callable = add_trig,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.spark_sliding_window import test_window_space
    task_spark_test_window_space = PythonOperator(
        task_id = 'task_spark_test_window_space',
        python_callable = test_window_space,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.spark_sliding_window import do_sliding_window
    task_spark_do_sliding_window = PythonOperator(
        task_id = 'task_spark_do_sliding_window',
        python_callable = do_sliding_window,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.spark_sliding_window import QA_sliding_window_positions
    task_spark_QA_sliding_window_positions = PythonOperator(
        task_id = 'task_spark_QA_sliding_window_positions',
        python_callable = QA_sliding_window_positions,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.array_expand import task_expand_arrays
    task_spark_expand_arrays = PythonOperator(
        task_id = 'task_spark_expand_arrays',
        python_callable = task_expand_arrays,
        **common_kwargs,
    )

    from forex.pre_training_data_prep.tasks.spark.task_deal_with_post_sliding_window_NaNs import deal_with_post_sliding_window_nans
    task_spark_deal_with_post_sliding_window_nans = PythonOperator(
        task_id = 'task_spark_deal_with_post_sliding_window_nans',
        python_callable = deal_with_post_sliding_window_nans,
        **common_kwargs,
    )
    
    
    ###############################
    #   Assemble DAG from tasks   #
    ###############################

    [ task_prepare_DAG_run ] >> task_pull_forex_data

    [ task_pull_forex_data ] >> task_add_timezone_information

    [ task_add_timezone_information, task_generate_offset_map ] >> task_merge_offset_map >> task_shift_days_and_hours_as_needed >> task_finalize_pandas_candlesticks >> task_spark_convert_pandas_df_to_spark_df >> task_spark_pivot_and_sort_arrays >> task_spark_diff_the_timestamp_arrays >> task_spark_find_full_day_nans >> task_spark_add_trig >> task_spark_test_window_space >> task_spark_do_sliding_window >> task_spark_expand_arrays >> task_spark_deal_with_post_sliding_window_nans

    [ task_spark_find_full_day_nans ] >> task_QA_full_day_nans
    [ task_spark_find_full_day_nans ] >> task_QA_full_day_consecutive_nans

    [ task_spark_do_sliding_window ] >> task_spark_QA_sliding_window_positions
    
    
#
# Enable command-line execution
#
if __name__ == '__main__':
    dag.test()


