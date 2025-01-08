# Nextflow Demos

## Forecasting EC2 Spot Prices

The Nextflow script "ec2_spot_price_analysis.nf" runs a series of scripts that

- Updates a database with the latest EC2 spot price information
- Groups and then resamples the spot price information to obtain a time series consisting of each day's mean price value
- Creates an ARIMA model of the time series using heuristically derived parameters
- Creates an ARIMA model using a stepwise search to derive parameters
- Compares the two ARIMA models

