import pendulum
from airflow.decorators import dag, task

@dag(
    dag_id = 'dumb_test',
    schedule = None,
    start_date = pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup = False,
    tags=['example'],
)
def DumbTest():
    """
    Blah Blah Blah.
    """

    @task()
    def extract_candlestick_data_from_database():
        """
        Meh.
        """

        #
        # TEMP until I figure out how to do this in airflow
        #
        pipeline_home = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components'
        import sys;
        sys.path.append(pipeline_home)

        #
        # load the libraries we need
        #
        from get_database_connection_string import db_connection_str
        from get_sql_for_pull import get_candlestick_pull_query
        from pull_data_from_database import pull_candlesticks_into_pandas_dataframe
        from pull_data_from_database import save_candlesticks_pandas_dataframe

        #
        # run task and return the data produced
        #
        sql_query_for_candlestick_pull = get_candlestick_pull_query()
        pdf = pull_candlesticks_into_pandas_dataframe(db_connection_str, sql_query_for_candlestick_pull)
        full_output_path = save_candlesticks_pandas_dataframe(pdf, pipeline_home)
        to_return = {'initial_candlesticks_pdf' : pdf, 'initial_candlesticks_pdf_full_output_path' : full_output_path}
        return to_return

    #
    # we are not currently using this;
    # it is leftover from the tutorial I used
    # and I'm keeping it for a slight bit longer
    # as a reference
    #
    @task(multiple_outputs = True)
    def transform(candlestick_data_dict: dict):
        """
        Meh.
        """
        return {'key' : 'words words words'}

    #
    # define pipeline component order and dependencies
    #
    candlestick_data_dict = extract_candlestick_data_from_database()
    temporary_transform = transform(candlestick_data_dict)

#
# declare a dag object
#
dag = DumbTest()

#
# main function (for testing the dag object)
#
if __name__ == '__main__':
    dag.test()
