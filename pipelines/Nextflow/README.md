# Nextflow Demos

***Note:***  I wrote the pipeline described herein when I was two days into learning Nextflow. Therefore this code may not represent the most optimal way to express the indended pipeline--a matter I will resolve shortly. This being said, these Nextflow scripts do run successfully:

## Forecasting EC2 Spot Prices

The Nextflow script [ec2_spot_price_analysis.nf](ec2_spot_price_analysis.nf) runs a series of scripts that

- Updates a database with the latest EC2 spot price information
- Groups and then resamples the spot price information to obtain a time series consisting of each day's mean price value
- Creates an ARIMA model of the time series using heuristically derived parameters
- Creates an ARIMA model using a stepwise search to derive parameters
- Compares the two ARIMA models

Example output can be found in the subdirectory [saved_output_examples/ec2_spot_price_analysis](saved_output_examples/ec2_spot_price_analysis) which contains the final output of this Nextflow pipeline expressed as a Jupyter notebook along with a snapshot of the Django database used to produce these results. Note that the required Django object model definition, as well as the scripts called by this Nextflow pipeline, are located in the [/badassdatascience/django/ec2_spot_price_tracker](/badassdatascience/django/ec2_spot_price_tracker). Once of these scripts, [run_notebook.py](/badassdatascience/django/ec2_spot_price_tracker/scripts/run_notebook.py), runs the following notebook from the command line:

- [badassdatascience/forecasting/ARIMA/ARIMA_demo_forecasting_EC2_spot_prices.ipynb](/badassdatascience/forecasting/ARIMA/ARIMA_demo_forecasting_EC2_spot_prices.ipynb)



