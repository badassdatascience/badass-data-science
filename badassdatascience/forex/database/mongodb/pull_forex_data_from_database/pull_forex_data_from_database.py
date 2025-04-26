import pandas as pd


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
