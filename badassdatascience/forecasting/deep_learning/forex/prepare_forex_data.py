

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
    
    
    ###############################
    #   Assemble DAG from tasks   #
    ###############################

    [ task_prepare_DAG_run ] >> task_pull_forex_data
    [ task_pull_forex_data ] >> task_add_timezone_information
    [ task_add_timezone_information, task_generate_offset_map ] >> task_merge_offset_map >> task_shift_days_and_hours_as_needed >> task_finalize_pandas_candlesticks
    
    
#
# Enable command-line execution
#
if __name__ == '__main__':
    dag.test()



