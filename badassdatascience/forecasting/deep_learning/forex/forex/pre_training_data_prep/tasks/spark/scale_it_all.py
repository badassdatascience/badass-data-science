import numpy as np

import pyspark.sql.functions as f
from pyspark.sql.types import FloatType

from utilities.spark_session import get_spark_session

def compute_mean_sans_nans(the_list):
    return float(np.nanmean(the_list))

udf_compute_mean_sans_nans = f.udf(compute_mean_sans_nans, FloatType())

def compute_std_sans_nans(the_list):
    return float(np.nanstd(the_list))

udf_compute_std_sans_nans = f.udf(compute_std_sans_nans, FloatType())

def compute_scaling_statistics_for_later_use(**config):
    spark = get_spark_session(config['spark_config'])
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + config['filename_post_nan_filters'])

    for item in config['list_data_columns']:
        sdf_arrays = (
            sdf_arrays
            .withColumn('mean_' + item, udf_compute_mean_sans_nans(f.col(item)))
            .withColumn('std_' + item, udf_compute_std_sans_nans(f.col(item)))
        )

    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + config['filename_scaling_stats'])
    spark.stop()
