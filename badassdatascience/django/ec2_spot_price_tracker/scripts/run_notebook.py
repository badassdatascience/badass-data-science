

import papermill as pm

pm.execute_notebook(
    '/home/emily/Desktop/projects/test/badassdatascience/badassdatascience/forecasting/ARIMA/test_it.ipynb',
    'test_it_output.ipynb',
    parameters = dict(
	filename_parquet = 'resampled.parquet'
    )
)
