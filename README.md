# <a name="top-badass-data-science-repository"></a>Badass Data Science

This repository contains multiple skills demonstrations (designed to assist my employment search) as well as code supporting my [Badass Data Science](https://badassdatascience.com) blog.

Note: The Badass Data Science blog is currently down for refactoring; I'll republish it soon.

## Skill Demonstrations by Subject Matter

#### <a name="forecasting" target="_blank"></a>Forecasting

###### <a name="forecasting-ARIMA-ec2-spot-prices" target="_blank"></a>ARIMA Modeling of EC2 Spot Prices

This section consists of a Jupyter notebook that forecasts EC2 spot prices using ARIMA models:

- <a name="arima-ec2-spot-prices" target="_blank" href="badassdatascience/forecasting/ARIMA/ARIMA_demo_forecasting_EC2_spot_prices.ipynb">ARIMA_demo_forecasting_EC2_spot_prices.ipynb</a>

While this notebook can be run from the Jupyter GUI, I also provide a Nextflow pipeline that updates the EC2 spot price database and resamples its contents. This is designed to be run once per day:

- [Nextflow/ec2_spot_price_analysis.nf](Nextflow/ec2_spot_price_analysis.nf)

Finally, a Django model provides the object modeling infrastructure for both the database and the queries made using it:

- [badassdatascience/django/ec2_spot_price_tracker/models.py](badassdatascience/django/ec2_spot_price_tracker/models.py)

Here is example output:
- [Sample EC2 spot price forecast results](Nextflow/saved_output_examples/ec2_spot_price_analysis/NEXTFLOW_OUTPUT_ARIMA_demo_forecasting_EC2_spot_prices_2025-01-07.ipynb)

#### <a name="nextflow"></a>Nextflow

See the [ARIMA modeling of EC2 Spot Prices](#forecasting-ARIMA-ec2-spot-prices) in the [Forecasting](#forecasting) section of this document to see an example of using Nextflow to control this forecasting pipeline. This pipeline coordinates the whole forecasting process from data update to resampling to time series analysis.

#### <a name="object-modeling-django"></a>Object Modeling

See the [ARIMA modeling of EC2 Spot Prices](#forecasting-ARIMA-ec2-spot-prices) in the [Forecasting](#forecasting) section of this document to see how I use Django for object modeling of time series data. When run in conjunction with Nextflow, the result is an ETL pipeline that prepares time series data for the EC2 spot price forecasting task.

Note: One can get better ETLs (i.e., ETLs that are easier to maintain) by using an enterprise solution like Databricks.

## <a name="getting-the-code-to-run"></a>Getting the code to run

You'll first need to define the root location of this repository using the "BDS_HOME" e.g.:

```
export BDS_HOME=/home/user/Desktop/projects/badassdatascience
```


