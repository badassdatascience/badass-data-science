import numpy as np
import matplotlib.pyplot as plt
from itertools import groupby

import pyspark.sql.functions as f
from pyspark.sql.types import IntegerType, BooleanType

from utilities.spark_session import get_spark_session


def qa_full_day_nans(**config):
    spark = get_spark_session(config['spark_config'])

    run_id = config['dag_run'].run_id

    def count_nans_in_array(values_array):
        values_array = np.array([np.array(values_array)])
        mask = np.isnan(values_array)
        nan_count = np.sum([int(x) for x in mask[0]])
        return int(nan_count)

    udf_count_nans_in_array = f.udf(count_nans_in_array, IntegerType())

    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + run_id + '/' + config['filename_full_day_nans'])
    for item in config['list_data_columns']:
        sdf_arrays = (
            sdf_arrays
            .withColumn(
                item + '_nan_count',
                udf_count_nans_in_array(f.col(item + '_and_nans'))
            )
        )
    sdf_arrays = sdf_arrays.withColumn('nan_count_full_day', f.col(config['list_data_columns'][0] + '_nan_count'))
    for item in config['list_data_columns']:
        sdf_arrays = sdf_arrays.drop(item + '_nan_count')

    
    sdf_qa = (
        sdf_arrays
        .select('nan_count_full_day', 'date_post_shift')
        .orderBy(f.col('nan_count_full_day').desc())
        .withColumn('day_has_nans', f.col('nan_count_full_day') > 0)
        .groupBy('day_has_nans').agg(f.count('date_post_shift').alias('number_of_days'))
    )
    sdf_qa.write.mode('overwrite').parquet(config['directory_output'] + '/' + run_id + '/QA/' + config['filename_qa_day_has_nans'])

    sdf_qa_counts = (
        sdf_arrays
        .select('date_post_shift', 'nan_count_full_day')
        .orderBy(f.col('nan_count_full_day').desc())
    )
    sdf_qa_counts.write.mode('overwrite').parquet(config['directory_output'] + '/' + run_id + '/QA/' + config['filename_qa_day_nan_counts'])

    # boxplots
    nan_counts_per_day = [x['nan_count_full_day'] for x in sdf_qa_counts.select('nan_count_full_day').collect()]
    plt.figure()
    plt.boxplot([nan_counts_per_day], widths=0.95)
    plt.savefig(config['directory_output'] + '/' + run_id + '/QA/' + config['filename_qa_full_day_boxplot'])
    plt.close()

    pdf_qa_dates = sdf_qa_counts.toPandas()
    pdf_qa_dates['weekday'] = [x.weekday() for x in pdf_qa_dates['date_post_shift']]
    pdf_qa_dates.boxplot(by = 'weekday')
    plt.savefig(config['directory_output'] + '/' + run_id + '/QA/' + config['filename_qa_full_day_boxplot_per_day'])
    plt.close()


def qa_full_day_consecutive_nans(**config):
    #from utilities.test_all_equality import udf_test_all_equality

    def test_all_equality(*a_list):
        g = groupby(a_list)
        return next(g, True) and not next(g, False)

    udf_test_all_equality = f.udf(test_all_equality, BooleanType())

    
    def get_max_consecutive_NaNs(a_list):

        n_consec_nan_list = [0]
        count = 0
        is_in_nan_group = False
        for item in np.array(a_list):
            if np.isnan(item) or item == None:
                is_in_nan_group = True
                count += 1
            if not np.isnan(item) and is_in_nan_group:
                is_in_nan_group = False
                n_consec_nan_list.append(count)
                count = 0

        return max(n_consec_nan_list)

    udf_get_max_consecutive_NaNs = f.udf(get_max_consecutive_NaNs, IntegerType())
    

    
    spark = get_spark_session(config['spark_config'])
    run_id = config['dag_run'].run_id
    sdf_arrays = spark.read.parquet(config['directory_output'] + '/' + run_id + '/' + config['filename_full_day_nans'])

    for item in config['list_data_columns']:
        sdf_arrays = sdf_arrays.withColumn('max_consec_nan_' + item, udf_get_max_consecutive_NaNs(f.col(item + '_and_nans')))

    columns_to_test = [f.col('max_consec_nan_' + x) for x in config['list_data_columns']]
    sdf_arrays = sdf_arrays.withColumn('is_consec_equal', udf_test_all_equality(*columns_to_test))

    sdf_arrays = (
        sdf_arrays
        .withColumn('max_daily_consec_nans', f.col('max_consec_nan_' + config['list_data_columns'][0]))
    )

    # clean up
    sdf_arrays = sdf_arrays.drop('timestamps_all')
    for item in config['list_data_columns']:
        sdf_arrays = (
            sdf_arrays
            .drop(
                item + '_and_nans',
            )
        )

    sdf_arrays.write.mode('overwrite').parquet(config['directory_output'] + '/' + run_id + '/QA/' + config['filename_qa_full_day_consecutive_nans'])
    
    spark.stop()                                       
