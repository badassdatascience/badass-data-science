#
# Load useful modules
#
import pandas as pd
import pandas_ta as ta  # ta.version = '0.3.14b0'
import warnings
warnings.filterwarnings('ignore')

#
# Indicators to compute
#
emily_ta_kinds = [
    {'kind': 'aberration'},
    {'kind': 'apo'},
    {'kind': 'cci'},
    {'kind': 'cmf'},
    {'kind': 'dema'},
    {'kind': 'eri'},
    {'kind': 'aroon'},
    {'kind': 'hma'},
    {'kind': 'donchian'},
    {'kind': 'kc'},
    {'kind': 'cmo'},
    {'kind': 'coppock'},
    {'kind': 'dpo'},
    {'kind': 'fisher'},
    {'kind': 'efi'},
    {'kind': 'ema'},
    {'kind': 'entropy'},
    {'kind': 'eom'},
    {'kind': 'er'},
    {'kind': 'atr'},
    {'kind': 'macd'},
    {'kind': 'mad'},
    {'kind': 'fwma'},
    {'kind': 'ha'},
    #{'kind': 'cdl_doji'},
    #{'kind': 'cdl_inside'},
    
    #{'kind': 'crossed'},
    #{'kind': 'cross'},  # something about "a" and "b"
    
    {'kind': 'midprice'},
    {'kind': 'mom'},
    {'kind': 'accbands'},
    {'kind': 'natr'},
    {'kind': 'ad'},
    {'kind': 'adosc'},
    {'kind': 'massi'},
    {'kind': 'adx'},
    #{'kind': 'mcgd'},
    {'kind': 'kdj'},
    #{'kind': 'keltner_channel'},
    {'kind': 'pgo'},
    {'kind': 'bbands'},
    {'kind': 'bias'},
    #{'kind': 'bollinger_bands'},
    {'kind': 'pvt'},
    {'kind': 'pwma'},
    {'kind': 'qqe'},
    {'kind': 'cfo'},
    {'kind': 'nvi'},
    {'kind': 'amat'},
    {'kind': 'ppo'},
    {'kind': 'psar'},
    {'kind': 'ao'},
    {'kind': 'aobv'},
    {'kind': 'obv'},
    #{'kind': 'hull_moving_average'},
    #{'kind': 'rolling_max'},
    {'kind': 'rvgi'},
    {'kind': 'squeeze'},
    {'kind': 'rvi'},
    {'kind': 'ohlc4'},
    {'kind': 'pdist'},
    {'kind': 'percent_return'},
    
    #{'kind': 'tdi'},
    {'kind': 'tsi'},
    
    #{'kind': 'typical_price'},
    {'kind': 'wcp'},
    #{'kind': 'weighted_bollinger_bands'},
    
    #{'kind': 'zlwma'},
    {'kind': 'zlma'},
    
    #{'kind': 'heikinashi'},
    {'kind': 'psl'},
    {'kind': 'ssf'},
    {'kind': 'pvi'},
    {'kind': 'pvo'},
    {'kind': 'pvol'},
    {'kind': 'pvr'},
    #{'kind': 'session'},
    {'kind': 'median'},
    {'kind': 'mfi'},
    
    #{'kind': 'mid_price'},
    {'kind': 'midprice'},
    
    {'kind': 'stdev'},
    {'kind': 'stoch'},
    {'kind': 'stochrsi'},
    {'kind': 'supertrend'},
    {'kind': 'swma'},
    {'kind': 't3'},
    {'kind': 'cg'},
    {'kind': 'chop'},
    #{'kind': 'chopiness'},
    {'kind': 'qstick'},
    {'kind': 'quantile'},
    #{'kind': 'returns'},
]

#
# Define a custom indicator list object
#
EmilyStrategy = ta.Strategy(
    name = "Emily's Custom Indicators",
    description = '',
    ta = emily_ta_kinds,
)

candlestick_components_name_dict = {
    'o' : 'open',
    'h' : 'high',
    'l' : 'low',
    'c' : 'close',
}

candlestick_components_list = ['o', 'h', 'l', 'c']

def compute_ta_indicators(
    df,
    price_type_list = ['ask', 'mid', 'bid'],
    strategy_to_use = EmilyStrategy,
    candlestick_components_list = candlestick_components_list,
    candlestick_components_name_dict = candlestick_components_name_dict,
):
    df_by_price_type_list = []
    for price_type in price_type_list:
        initial_columns_list = [price_type + '_' + q for q in candlestick_components_list]
        df_price_type_temp = df[initial_columns_list].copy()
        df_price_type_temp.columns = [
            candlestick_components_name_dict[q] for q in candlestick_components_list
        ]
        df_price_type_temp['volume'] = df['volume']
        df_price_type_temp['date'] = pd.to_datetime(df['time_iso_pd_datetime'], utc = True)
        df_price_type_temp.sort_values(by = 'date', inplace = True)
        df_price_type_temp.set_index('date', inplace = True)
        df_price_type_temp.ta.strategy(strategy_to_use)
        df_price_type_temp.drop(columns = ['open', 'high', 'low', 'close', 'volume'], inplace = True)
        df_price_type_temp.columns = [price_type + '_' + q for q in df_price_type_temp.columns]
        df_by_price_type_list.append(df_price_type_temp)

    df_to_return = df_by_price_type_list[0]
    for df_temp in df_by_price_type_list[1:]:
        df_to_return = df_to_return.join(df_temp)
    
    df_to_return = df_to_return.reset_index().drop(columns = ['date']).copy()
    column_list_sans_time = list(df_to_return.columns)
    
    df_to_return['time'] = df['time'].values
    column_list = ['time']
    column_list.extend(column_list_sans_time)
    df_to_return = df_to_return[column_list].copy()
    
    return df_to_return
