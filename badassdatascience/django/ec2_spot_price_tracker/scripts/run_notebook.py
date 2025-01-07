import papermill as pm

pm.execute_notebook(
    '/home/emily/Desktop/projects/test/badassdatascience/badassdatascience/forecasting/ARIMA/ARIMA_demo_forecasting_EC2_spot_prices.ipynb',
    'NEXTFLOW_OUTPUT_ARIMA_demo_forecasting_EC2_spot_prices.ipynb',
    parameters = dict(
	filename_parquet = 'resampled.parquet'
    )
)
