
import pandas as pd
from statsmodels.tsa.stattools import adfuller

df = pd.read_parquet('resampled.parquet')
result = adfuller(df['spot_price'])
print('metric,value')
print('ADF Statistic,' + str(result[0]))
print('p-value,' + str(result[1]))
