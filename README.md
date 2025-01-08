# Badass Data Science

This repository contains multiple skills demonstrations--to assist my employment search--as well as code supporting my [Badass Data Science](https://badassdatascience.com) blog.

Note: The Badass Data Science blog is currently down for refactoring; I'll republish it soon.

## Demos by subject matter

### Forecasting

#### ARIMA Modeling of EC2 Spot Prices

This consists of a Jupyter notebook that forecasts EC2 spot prices using ARIMA models:

- badassdatascience/forecasting/ARIMA/ARIMA_demo_forecasting_EC2_spot_prices.ipynb

While this notebook can be run from the Jupyter GUI, I also provide a Nextflow pipeline that updates the EC2 spot price database and resamples its contents. This is designed to be run once per day:

- Nextflow/ec2_spot_price_analysis.nf

Finally, a Django model provides the object modeling infrastructure for both the database and the queries made using it:

- badassdatascience/django/ec2_spot_price_tracker/models.py

### Nextflow

See the "ARIMA modeling of EC2 Spot Prices" in the "Forecasting" section of this document to see an example of using Nextflow to control this forecasting pipeline. This pipeline coordinates the whole forecasting process from data update to resampling to time series analysis.

### Object Modeling

See the "ARIMA modeling of EC2 Spot Prices" in the "Forecasting" section of this document to see how I use Django for object modeling of time series data. When run in conjunction with Nextflow, the result is an ETL pipeline that prepares time series data for the EC2 spot price forecasting task.

Note: One can get better ETLs (i.e., ETLs that are easier to maintain) by using an enterprise solution like Databricks.

## Getting the code to run

You'll first need to define the root location of this repository using the "BDS_HOME" e.g.:

```
export BDS_HOME=/home/user/Desktop/projects/badassdatascience
```


