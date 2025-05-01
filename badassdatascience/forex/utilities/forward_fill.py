import itertools

def ff(
    df,
    time_column = 'time',
    price_type_list = ['ask', 'mid', 'bid'],
    element_list = ['o', 'h', 'l', 'c', 'return', 'volatility'],
):
    df = df.copy()
    df.sort_values(by = [time_column], inplace = True)
    columns_to_forward_fill = [q + '_' + r for q, r in list(itertools.product(price_type_list, element_list))]
    columns_to_forward_fill.append('volume')
    df_fill_columns = df[columns_to_forward_fill].ffill()

    for column_name in columns_to_forward_fill:
        df[column_name] = df_fill_columns[column_name]

    return df
