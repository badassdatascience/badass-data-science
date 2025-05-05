import pandas as pd
import pickle

from badassdatascience.forex.database.mongodb.utilities.connect_to_mongoDB import get_candlestick_documents

#
# we perform sorts in both MongoDB and Pandas, 
# which is probably overkill. However, doing 
# so doesn't cost us much in compute time
# and provides a greater degree of robustness
# to error
#
def pull_forex_into_df(candlesticks, instrument = 'EUR/USD', granularity = 'H1'):
    df = (
        pd.DataFrame(
            list(
                candlesticks
                .find(
                    {
                        'instrument' : instrument,
                        'granularity' : granularity,
                        'complete' : True,
                    },
                )
                .sort(
                    {
                        'instrument' : 1,
                        'granularity' : 1,
                        'time' : 1,
                        'complete' : 1,
                    }
                )
            )
        )
        .sort_values(
            by = ['instrument', 'granularity', 'time', 'complete'],
        )
        .drop(columns = ['complete'])
    )

    df['time_iso_pd_datetime'] = [
        pd.to_datetime(q, format='%Y-%m-%dT%H:%M:%S%z') for q in df['time_iso']
    ]
    return df


def query_single_instrument_and_single_granularity(
    instrument = 'EUR/USD',
    granularity = 'M15',
    pull_from_database = True,
    save_file = 'output/df.pickled',
):

    if pull_from_database:
        candlestick_documents = get_candlestick_documents()
        df = pull_forex_into_df(candlestick_documents, instrument = instrument, granularity = granularity)
        with open(save_file, 'wb') as fff:
            pickle.dump(df, fff)
    else:
        with open(save_file, 'rb') as fff:
            df = pickle.load(fff)

    return df
