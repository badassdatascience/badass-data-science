def does_df_have_only_one_instrument_and_granularity(df):
    return len(df.groupby(['instrument', 'granularity'])['_id'].agg('count').index) == 1
