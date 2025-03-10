import pandas as pd

from pyspark import SparkConf
from pyspark.sql import SparkSession

def get_spark_session(spark_config):

    #
    # move this to a config file
    #
    spark_config = SparkConf().setAll(spark_config)

    #
    # define spark session
    #
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .appName('forex_prep')
        .config(conf = spark_config)
        .getOrCreate()
    )

    spark.catalog.clearCache()   # Does this matter?
    return spark


def load_pandas_df_parquet_into_spark_df(
    filename,
    spark,
    truncate_to_row_number = None,
    n_processors_to_coalesce = None,
):
    if truncate_to_row_number == None:
        pdf_arrays = pd.read_parquet(filename)
    else:
        pdf_arrays = pd.read_parquet(filename).head(truncate_to_row_number)
    
    sdf = spark.createDataFrame(pdf_arrays)

    if n_processors_to_coalesce != None:
        sdf = sdf.coalesce(n_processors_to_coalesce)
    
    return(sdf)
