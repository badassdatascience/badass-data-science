#
# load useful system libraries
#
import numpy as np
import pandas as pd

#
# load useful repository libraries
#
from badassdatascience.utilities.badass_string_processing.badass_string_processing import split_string_by_digit

#
# Map granularity time unit specifiers to their values in seconds
#
# I worry that this doesn't enable accounting for leap seconds
# when used to compute Unix epoch times. This dictionary itself
# isn't the concern, but how it is used.
#
# Have to give this more thought...
#
dict_granularity_label_to_seconds_map = {
    'S' : 1,
    'M' : 60,
    'H' : 60 * 60,
}

#
# Define function to clean up and process Oanda's granularity list
# into a more useful DataFrame
#
def process_oanda_granularity_listings(input_file, dict_granularity_label_to_seconds_map):

    #
    # load the raw data
    #
    df_granularities = pd.read_csv(input_file, sep = '\t')

    #
    # convert all the column names to lowercase
    #
    df_granularities.columns = [q.strip().lower() for q in df_granularities.columns]

    #
    # remove any whitespace from each end
    #
    for col in df_granularities.columns:
        df_granularities[col] = [q.strip() for q in df_granularities[col]]

    #
    # initialize results list
    #
    # This will be used to produce the final Pandas DataFrame
    #
    list_dict_processed_oanda_granularities = []

    #
    # split each incoming row into alphabetical and numeric components
    #
    for oanda_granularity in df_granularities['value']:
        granularity_component_list = split_string_by_digit(oanda_granularity)
        granularity_component_list = [q.strip() for q in granularity_component_list if not q.strip() == '']

        #
        # determine if the granularity list is acceptable
        # for our intended use
        #
        is_this_row_useful = False
        if len(granularity_component_list) == 2:
            is_this_row_useful = (
                (granularity_component_list[0].isalpha()) &
                (granularity_component_list[1].isdigit())
            )

        #
        # process the row to compute granularity value in seconds
        # for hour, minute, and second cases
        #
        if is_this_row_useful:
            key = granularity_component_list[0]
            value = int(granularity_component_list[1])
            dict_assembly = {
                'value' : oanda_granularity,
                'hour_minute_or_second' : key,
                'seconds' : dict_granularity_label_to_seconds_map[key] * value,
            }
            
            list_dict_processed_oanda_granularities.append(dict_assembly)

    #
    # create the final DataFrame
    #
    df_granularities = (
        pd.merge(
            df_granularities,
            pd.DataFrame(list_dict_processed_oanda_granularities),
            on = 'value',
            how = 'left',
        )
        .rename(columns = {'value' : 'oanda_granularity'})
    )

    #
    # ...exit stage left
    #
    return df_granularities

#
# main
#
if __name__ == '__main__':

    import os
    
    # user settings
    raw_source_data_filename = os.environ['BDS_HOME'] + '/badassdatascience/forex/data/oanda_granularities_raw.txt'

    # create the dataframe
    df = process_oanda_granularity_listings(
        raw_source_data_filename,
        dict_granularity_label_to_seconds_map,
    )

    print(df)
        
