import itertools
import datetime
from pytz import timezone
import matplotlib.pyplot as plt

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


def test_post_ff(df_post_ff, timezone_name = 'US/Eastern'):
    tz = timezone(timezone_name)
    
    # 16:00 rather than 17:00 because of daylight savings time
    weekend_start_method_one = datetime.datetime(2025, 4, 25, 16, 0, 0, 0, tzinfo = tz).timestamp()
    weekend_end_method_one = datetime.datetime(2025, 4, 27, 16, 0, 0, 0, tzinfo = tz).timestamp()

    df_weekend_ff_test_one = df_post_ff[
        (df_post_ff['time'] >= weekend_start_method_one) & 
        (df_post_ff['time'] < weekend_end_method_one)
    ]

    # check immediate surroundings of the previous interval
    weekend_start_method_two = datetime.datetime(2025, 4, 25, 15, 0, 0, 0, tzinfo = tz).timestamp()
    weekend_end_method_two = datetime.datetime(2025, 4, 27, 17, 0, 0, 0, tzinfo = tz).timestamp()

    df_weekend_ff_test_two = df_post_ff[
        (df_post_ff['time'] >= weekend_start_method_two) & 
        (df_post_ff['time'] < weekend_end_method_two)
    ]

    # final test
    x_start = datetime.datetime(2025, 4, 24, 15, 0, 0, 0, tzinfo = tz).timestamp()
    x_end = datetime.datetime(2025, 4, 30, 15, 0, 0, 0, tzinfo = tz).timestamp()
    df_ff_final_test = df_post_ff[
        (df_post_ff['time'] >= x_start) & 
        (df_post_ff['time'] < x_end)
    ]

    return df_weekend_ff_test_one, df_weekend_ff_test_two, df_ff_final_test

def rough_ff_plot(x, y):
    plt.figure()
    plt.plot(x, y)
    plt.show()
    plt.close()

def ff_test_plots(df_post_ff):
    df_weekend_ff_test_one, df_weekend_ff_test_two, df_ff_final_test = test_post_ff(df_post_ff)
    
    rough_ff_plot(df_weekend_ff_test_one['time'], df_weekend_ff_test_one['ask_c'])
    rough_ff_plot(df_weekend_ff_test_two['time'], df_weekend_ff_test_two['ask_c'])
    rough_ff_plot(df_ff_final_test['time'], df_ff_final_test['ask_c'])