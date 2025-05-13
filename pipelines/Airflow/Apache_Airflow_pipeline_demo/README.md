# Pipeline Skills Demo (Apache Airflow)

The code contained herein implements an pipeline that prepares raw currency price data for forecasting using deep learning. Here is a schematic of the pipelines directed acyclic graph (DAG):

!["airflow-screenshot"](Airflow-Screenshot.png)

We define DAG elements (tasks) in Python script [pipelines/Airflow/Apache_Airflow_pipeline_demo/prepare_forex_data.py](pipelines/Airflow/Apache_Airflow_pipeline_demo/prepare_forex_data.py) which provides the "command and control" for this pipeline.

