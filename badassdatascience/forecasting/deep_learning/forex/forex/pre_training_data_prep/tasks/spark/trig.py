import numpy as np

import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, FloatType

from utilities.spark_session import get_spark_session

seasonal_forecasting_period = 60. * 60. * 24.
seasonal_forecasting_frequency = (2. * np.pi) / seasonal_forecasting_period
seasonal_forecasting_amplitude = 1.

def sin_24_hours(timestamp_array_in_seconds):
    result = seasonal_forecasting_amplitude * np.sin(seasonal_forecasting_frequency * np.array(timestamp_array_in_seconds))
    return [float(x) for x in result]

udf_sin_24_hours = f.udf(sin_24_hours, ArrayType(FloatType()))

def cos_24_hours(timestamp_array_in_seconds):
    result = seasonal_forecasting_amplitude * np.cos(seasonal_forecasting_frequency * np.array(timestamp_array_in_seconds))
    return [float(x) for x in result]

udf_cos_24_hours = f.udf(cos_24_hours, ArrayType(FloatType()))


from utilities.spark_session import get_spark_session

def add_trig(**config):
    spark = get_spark_session(config['spark_config'])
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + 'PLACEHOLDER.parquet')

    sdf_arrays = (
        sdf_arrays
        .withColumn('sin_24', udf_sin_24_hours(f.col('timestamps_all')))
        .withColumn('cos_24', udf_cos_24_hours(f.col('timestamps_all')))
    )
    
    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['filename_post_trig'])
    spark.stop()
