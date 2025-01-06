
import pandas as pd
from statsmodels.tsa.stattools import adfuller

df = pd.read_parquet('resampled.parquet')
result = adfuller(df['spot_price'])
print('ADF Statistic:', result[0])
print('p-value:', result[1])
