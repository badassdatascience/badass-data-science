#
# load useful libraries
#
import uuid
from sqlalchemy import create_engine
import pandas as pd

from get_database_connection_string import db_connection_str

#
# temporary user settings
#
pipeline_home = '/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/pipeline_components'

#
# define query to run
#
sql_query_to_run = """SELECT

ts.timestamp, cs.o, cs.l, cs.h, cs.c, v.volume

FROM

timeseries_candlestick cs, timeseries_instrument inst,
timeseries_interval iv, timeseries_pricetype pt,
timeseries_volume v, timeseries_timestamp ts

WHERE

cs.instrument_id = inst.id
AND cs.interval_id = iv.id
AND cs.price_type_id = pt.id
AND cs.volume_id = v.id
AND cs.timestamp_id = ts.id

AND pt.name = '%s'
AND inst.name = '%s'
AND iv.name = '%s'

ORDER BY timestamp
;
"""

#
# pull candlesticks and save
#
# unfortunately, this design has the side effect of
# saving a pandas dataframe
#
def pull_candlesticks_and_save(
        pipeline_home,
        df_connection_str,
        sql_query_to_run,
        
        price_type_name = 'mid',
        instrument_name = 'EUR/USD',
        interval_name = 'Minute',

        table_prefix = 'candlestick_query_results',
        output_directory_local_to_home = 'output',
        query_output_directory_local_to_output_directory = 'queries',
        verbose = False,
):
       
    #
    # compute save directory path (without the final filename yet)
    #
    output_query_results_directory = '/'.join(
        [
            pipeline_home,
            output_directory_local_to_home,
            query_output_directory_local_to_output_directory,
        ]
    )

    #
    # substitute the "%s" query parameters with their intended values
    #
    sql_query_to_run = sql_query_to_run % (price_type_name, instrument_name, interval_name)
    
    #
    # connect to database
    #
    db_connection = create_engine(db_connection_str)

    #
    # run query, loading contents into a pandas dataframe
    #
    pdf = pd.read_sql(sql_query_to_run, con = db_connection)

    #
    # save output
    #
    uid = str(uuid.uuid4())
    output_filename = '%s_%s.parquet' % (table_prefix, uid)
    full_output_path = '/'.join([output_query_results_directory, output_filename])
    pdf.to_parquet(full_output_path)
    
    #
    # return something
    #
    return full_output_path

#
# run main
#
if __name__ == '__main__':
    full_output_path = pull_candlesticks_and_save(
        pipeline_home,
        db_connection_str,
        sql_query_to_run,
    )
    print(full_output_path)
