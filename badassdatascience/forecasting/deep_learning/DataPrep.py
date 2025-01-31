
# Questions to resolve
# Why aren't the seasonal functions output values between -1 and 1?
# Why is 1 added for max in the aggregation ?


#
# Load useful "system" libraries
#
import pickle
import pandas as pd
import numpy as np
import datetime
import pytz
import uuid
import sys
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, FloatType


#
# Load "local" libraries
#

application_root_directory = os.environ['BDS_HOME']
sys.path.append(application_root_directory)

application_django_directory = application_root_directory + '/badassdatascience/django'
sys.path.append(application_django_directory)

import boilerplate

#
# which nan function do we keep?
#
from utilities.make_dataframe_from_db import make_candlestick_dataframe
from utilities.date_and_time_related_calculations import compute_datetime_information
from utilities.nan_count_spark import nan_count_spark
from utilities.basic import udf_difference_an_array, udf_deal_with_offset, nan_helper, nan_count_spark

from utilities.seasonal_calculations import udf_normalized_spark_friendly_sine_with_24_hour_period
from utilities.seasonal_calculations import udf_normalized_spark_friendly_cosine_with_24_hour_period

from secret_sauce.secret_sauce import secret_sauce_inator




#
# Set configuration (hard-coded for now...)
#
config = {
    'verbose' : True,

    'price_type_name' : 'mid',
    'instrument_name' : 'EUR/USD',
    'interval_name' : 'Minute',

    'tz' : pytz.timezone('US/Eastern'),

    'output_directory' : os.environ['APP_HOME'] + '/output',  # maybe make APP_HOME part of the class definition
    'shifted_weekday_lookup_table_filename' : 'df_weekday_shift_lookup_table.csv',  # fix this directory structure
    'seconds_divisor' : 60, 
    #'diff_sum_cutoff' : 1000, # do we use this?

    'n_forward' : 15,
    'n_back' : 120,
    'n_step' : 100,

    'shuffle_it' : True,
    'modulus_integer' : 30,
    
    'number_of_secret_sauce_columns_to_use' : 1,
    
    #'column_names_list' : ['corrected_offset_price', 'corrected_offset_volume'], # do we need this?

    #'run_qa' : False,  # where is this used?
    
    'seed_numpy' : 54,

    'train_val_test_split' : {
        'train' : 0.75,
        'val' : 0.125,
        'test' : 0.125,
    },

    'reduce_vector_sizes' : True,
    
    'spark_configuration' : SparkConf().setAll(
        [
            ('spark.executor.memory', '15g'),
            ('spark.executor.cores', '3'),
            ('spark.cores.max', '3'),
            ('spark.driver.memory', '15g'),
            ('spark.sql.execution.arrow.pyspark.enabled', 'true'),
        ]
    ),
}




#####################################
#   Define data preparation class   #
#####################################

class DataPrep():




    
    # Probably should use a formal logging system than an included function
    def verbose(self, message):
        if self.config['verbose']:
            print()
            print(message)
            print()

    def verbose_DF(self, df, n = 5):
        if self.config['verbose']:
            print()
            print(type(df))
            print()
            if str(type(df)).find('pyspark.sql.dataframe.DataFrame') >= 0:
                df.show(n)
            if str(type(df)).find('pandas.core.frame.DataFrame') >= 0:
                print(df.head(n))
            print()
            








    # Constructor
    def __init__(self, spark, **kwargs):

        # store the config
        self.config = kwargs
        
        # get a distinct UUID for this data preparation run
        self.verbose('Assigning UUID...')
        self.uuid = str(uuid.uuid4())

        # connect to Spark
        self.verbose('Connecting to Spark...')
        self.spark = spark

        # random seed for numpy
        self.verbose('Setting the NumPy random seed...')
        np.random.seed(self.config['seed_numpy'])

        # # create initial Pandas dataframe
        # self.verbose('Creating the initial Pandas dataframe...')
        # self.make_initial_pdf()
        # self.verbose_DF(self.initial_pandas_df)

        # # save waypoint
        # self.verbose('Saving a waypoint...')
        # self.save_initial_pdf()

        # load instead
        self.verbose('Loading initial pandas dataframe...')
        self.load_initial_pdf()
        self.verbose_DF(self.initial_pandas_df)

        # align timestamps with Toronto's trading hours
        self.verbose('Aligning timestamps to trading hours...')
        self.shift_days_and_hours_as_needed()
        self.verbose_DF(self.initial_pandas_df)
        
        #
        # Define UDFs
        #
        self.verbose('Defining UDFs...')
        self.udf_difference_an_array = udf_difference_an_array
        self.udf_deal_with_offset = udf_deal_with_offset
        self.udf_get_the_sine_for_full_day = udf_normalized_spark_friendly_sine_with_24_hour_period
        self.udf_get_the_cosine_for_full_day = udf_normalized_spark_friendly_cosine_with_24_hour_period
        self.nan_helper = nan_helper
       
        # We are about to do some heavy lifting:
        self.verbose('Moving to Spark for heavy computational lifting...')
        self.move_to_spark()
        self.verbose_DF(self.all_possible_timestamps_spark_df)
       
        # this helps with interpolation and ensures timestamp gaps are dealt with properly
        self.verbose('Differencing the timestamps...')
        self.difference_the_timestamps()
        self.verbose_DF(self.arrays_spark_df)

        # add seasonal terms
        self.verbose('Adding seasonal terms...')
        self.add_seasonal_terms()
        self.verbose_DF(self.arrays_spark_df)

        # investigate array lengths after an aggregation
        self.investigate_array_lengths_after_aggregation()
        self.verbose_DF(self.arrays_spark_df)
        sys.exit(0)
        

        # ensure time series aligns with timestamps (mind the gap(s)!)
        self.verbose('Ensuring the time series aligns with the timestamps (minding the gap(s)...')
        self.correct_offset()
        self.verbose_DF(self.arrays_spark_df)

        #sys.exit(0)

        
        
        # we now move to NumPy to produce Keras-ready data
        self.verbose('Moving to NumPy to produce Keras-ready data...')
        self.move_to_NumPy()


        #self.arrays_spark_df.collect(); import sys; sys.exit(0) # test
        
        
        # interpolation and signal preparation
        self.verbose('Interpolating and signal prep...')
        self.define_signals_and_interpolate_missing_values()

        # get the "secret sauce"
        self.verbose('Adding the "secret sauce"...')
        self.MSS = secret_sauce_inator(self.X_all, self.config['number_of_secret_sauce_columns_to_use'])

        # scale everything
        self.verbose('Scaling everything...')
        self.scaled_dict = {}
        self.scale_it()

        # shuffle (optional)
        self.verbose('Shuffling...')
        if self.config['shuffle_it']:
            self.shuffle_it()

        # Assemble "final" Keras-friendly data structure
        self.verbose('Assembling "final" Keras-friendly data structure...')
        self.assemble_final_structure()

        # Reduce the number of rows in dataset (optional)
        self.verbose('Optionally reducing the row count...')
        self.reduce_data_size()

        # divide into training, validation, and test sets
        self.verbose('Dividing into training, validation, and test sets...')
        self.get_train_val_test()




















        
    #
    # We start in Pandas
    #
    def make_initial_pdf(self):
        pdf = (
            compute_datetime_information(
                make_candlestick_dataframe(self.config['price_type_name'], self.config['instrument_name'], self.config['interval_name']),
                self.config['tz'],
            )
        )

        pdf['timestamp'] = pdf.index

        pdf = (
            pd.merge(
                pdf,

                # FIX:
                #pd.read_csv(self.config['output_directory'] + '/' + self.config['shifted_weekday_lookup_table_filename']),
                pd.read_csv('/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/output/df_weekday_shift_lookup_table.csv'),

                on = ['weekday_tz', 'hour_tz'],
                how = 'left',
            )
            .sort_values(by = 'datetime_tz')
        )

        pdf['lhc'] = pdf[['l', 'h', 'c']].mean(axis=1)
        pdf = pdf[['timestamp', 'datetime_tz', 'weekday_tz', 'hour_tz', 'weekday_shifted', 'lhc', 'volume']].copy()
        self.initial_pandas_df = pdf

    def save_initial_pdf(self):
        self.initial_pandas_df.to_csv('output_initial_pdf.csv', index=False)
        self.initial_pandas_df.to_parquet('output_initial_pdf.parquet')
        with open('output_initial_pdf.pickle', 'wb') as f:
            pickle.dump(self.initial_pandas_df, f)
        
    def load_initial_pdf(self):
        self.initial_pandas_df = pd.read_parquet('output_initial_pdf.parquet')
            
    #
    # Quality analysis
    #
    def qa(self):
        print(self.initial_pandas_df.isnull().values.ravel().sum())
        
        print(nan_count_spark(self.all_possible_timestamps_spark_df, 'volume').take(1)[0]['volume_NaN_count'])
                
        # https://stackoverflow.com/questions/63565196/how-to-filter-in-rows-where-any-column-is-null-in-pyspark-dataframe
        print(
            self.all_possible_timestamps_spark_df
            .filter(
                f.greatest(
                    *[f.col(i).isNull() for i in self.all_possible_timestamps_spark_df.columns]
                )
            ).count()
        )

    
    def shift_days_and_hours_as_needed(self):
        self.initial_pandas_df['original_date'] = [x.date() for x in self.initial_pandas_df['datetime_tz']]
        self.initial_pandas_df['to_shift'] = self.initial_pandas_df['weekday_shifted'] - self.initial_pandas_df['weekday_tz']

        pdf_date_to_shift = (
            self.initial_pandas_df
            .sort_values(by = 'datetime_tz')
            [['weekday_tz', 'hour_tz', 'weekday_shifted', 'original_date', 'to_shift']]
            .drop_duplicates()
        )

        new_date_list = []
        for i, row in pdf_date_to_shift.iterrows():
            if row['to_shift'] > 0:
                delta = datetime.timedelta(days = row['to_shift'])
                new_date_list.append(row['original_date'] + delta)
            elif row['to_shift'] == -6:
                delta = datetime.timedelta(days = 1)
                new_date_list.append(row['original_date'] + delta)
            else:
                new_date_list.append(row['original_date'])

        pdf_date_to_shift['original_date_shifted'] = new_date_list

        pdf = (
            pd.merge(
                self.initial_pandas_df.drop(columns = ['to_shift']),
                pdf_date_to_shift,
                on = ['weekday_tz', 'hour_tz', 'weekday_shifted', 'original_date'],
                how = 'left',
            )
            .drop(columns = ['original_date', 'to_shift'])
            .sort_values(by = ['datetime_tz'])
        )

        self.initial_pandas_df = pdf.copy()

    
    def move_to_spark(self):
        
        self.initial_spark_df = self.spark.createDataFrame(self.initial_pandas_df)

        #
        # check to see if we used this later
        #
        self.timestamps_spark_df = (
            self.initial_spark_df
            .select('timestamp')
            .distinct()
            .withColumn('dummy_variable', f.lit(True))    
            .orderBy('timestamp')
        )

        self.all_possible_timestamps_spark_df = (
            self.timestamps_spark_df
            .join(
                self.initial_spark_df,
                ['timestamp'],
                'outer',
            )
            .drop('dummy_variable')
            .orderBy('timestamp')
        )



    def difference_the_timestamps(self):
        
        self.arrays_spark_df = (
            self.initial_spark_df
            .orderBy('datetime_tz')
            .groupBy('original_date_shifted')
            .agg(
                f.collect_list('lhc').alias('price'),
                f.collect_list('volume').alias('volume'),
                f.collect_list('timestamp').alias('timestamp')
            )
            .orderBy('original_date_shifted')

            .withColumn('array_length_price', f.size(f.col('price')))
            .withColumn('array_length_volume', f.size(f.col('volume')))
            .withColumn('array_length_timestamp', f.size(f.col('timestamp')))

            .withColumn(
                'length_test',
                (
                    (f.col('array_length_price') == f.col('array_length_volume')) &
                    (f.col('array_length_price') == f.col('array_length_timestamp'))
                )
            )
            .where(f.col('length_test') == True)
            .withColumnRenamed('array_length_price', 'array_length')
            .drop('array_length_volume', 'array_length_timestamp', 'length_test')
            
            .withColumn('seconds_divisor', f.lit(self.config['seconds_divisor']))
            .withColumn('diff_timestamp', self.udf_difference_an_array(f.col('timestamp'), f.col('seconds_divisor')))

            # maybe?
            .withColumn('array_length_diff_timestamp', f.size(f.col('diff_timestamp')))

            .drop('seconds_divisor')
            
            .orderBy('original_date_shifted')
        )

    def add_seasonal_terms(self):

        self.arrays_spark_df = (
            self.arrays_spark_df
            .withColumn('sine_for_full_day', self.udf_get_the_sine_for_full_day(f.col('timestamp')))
            .withColumn('cosine_for_full_day', self.udf_get_the_cosine_for_full_day(f.col('timestamp')))
            .orderBy('original_date_shifted')
        )

    def correct_offset(self):
        self.arrays_spark_df = (
            self.arrays_spark_df
            .withColumn('corrected_offset_price', self.udf_deal_with_offset(f.col('price'), f.col('diff_timestamp'), f.col('max_array_length')))
            .withColumn('corrected_offset_volume', self.udf_deal_with_offset(f.col('volume'), f.col('diff_timestamp'), f.col('max_array_length')))
            .withColumn('corrected_offset_sine_for_full_day', self.udf_deal_with_offset(f.col('sine_for_full_day'), f.col('diff_timestamp'), f.col('max_array_length')))
            .withColumn('corrected_offset_cosine_for_full_day', self.udf_deal_with_offset(f.col('cosine_for_full_day'), f.col('diff_timestamp'), f.col('max_array_length')))
            .withColumn('corrected_offset_length', f.size(f.col('corrected_offset_volume')))
            .drop(
                'price', 'volume', 'sine_for_full_day', 'cosine_for_full_day',
                'timestamp', 'diff_timestamp', 'diff_sum', 'max_array_length'
            )
        )
        
        

    def move_to_NumPy(self):
        # there is probably a better way to convert a 2D np.array to a 2D np.matrix:

        self.M_unscaled_dict = {}

        print()
        print(self.M_unscaled_dict)
        print(self.max_array_length)  # 1444
        print()
        
        for ci, column_name in enumerate(
                [
                    'corrected_offset_price', 'corrected_offset_volume', 'corrected_offset_sine_for_full_day', 'corrected_offset_cosine_for_full_day'
                ]
                ):

            M_pre = self.arrays_spark_df.select(column_name).toPandas().to_numpy()
            M = np.zeros([M_pre.shape[0], self.max_array_length])
            
            for i in range(0, M.shape[0]):
                M[i, :] = M_pre[i, 0]

            self.M_unscaled_dict[column_name] = M


    def define_signals_and_interpolate_missing_values(self):
        X_list = {}
        y_list = {}
        y_full_list = {}

        for signal_name in [
            'corrected_offset_price', 'corrected_offset_volume',
            'corrected_offset_sine_for_full_day', 'corrected_offset_cosine_for_full_day',
        ]:
            n_rows, n_cols = self.M_unscaled_dict[signal_name].shape

            X_list[signal_name] = []
            y_list[signal_name] = []
            y_full_list[signal_name] = []
    
            for r in range(0, n_rows):
                signal = self.M_unscaled_dict[signal_name][r]



                
                i = len(signal) - 1
                while np.isnan(signal[i]):
                    i -= 1

                    # refactor this
                    if i < 1:
                        break
                    
                    #print()
                    #print(signal.shape)
                    #print(i)
                    #print()
                    #import sys; sys.exit(0) # test



                    
                signal = signal[0:(i + 1)]

                nans, x = self.nan_helper(signal)

                print(nans.shape, x.shape, signal.shape)
                
                #if nans.shape[0] < 2:
                    #print(.shape, len(signal), len(x))
                    #print()
                signal[nans] = np.interp(x(nans), x(~nans), signal[~nans])

                for i in range(
                    self.config['n_back'],
                    len(signal) - self.config['n_back'] - self.config['n_forward']
                ):
                    back = np.array(signal[(i - self.config['n_back']):i])
                    forward = np.array(signal[i:(i + self.config['n_forward'])])
            
                    the_min = min(forward)
                    the_max = max(forward)
                    the_mean = np.mean(forward)
                    the_median = np.median(forward)
        
                    X_list[signal_name].append(back)
                    y_list[signal_name].append([the_min, the_mean, the_median, the_max])
                    y_full_list[signal_name].append(forward)

        self.X_all = np.array(X_list['corrected_offset_price'])
        self.X_volume_all = np.array(X_list['corrected_offset_volume'])
        self.X_sin_all = np.array(X_list['corrected_offset_sine_for_full_day'])
        self.X_cos_all = np.array(X_list['corrected_offset_cosine_for_full_day'])
        self.y_all = np.array(y_list['corrected_offset_price'])
        self.y_forward_all = np.array(y_full_list['corrected_offset_price'])
        self.row_count_all = self.X_all.shape[0]

    def scale_it(self):
        the_shape = self.X_all.shape

        for var, name in zip(
            [
                self.X_all,
                self.X_volume_all,
                self.MSS,
                self.y_forward_all,
            ], [
                'X_all_scaled',
                'X_volume_all_scaled',
                'MSS_all_scaled',
                'y_forward_all',
            ]
        ):
       
            M = np.zeros(the_shape)
            for q in range(0, the_shape[-1]):
                M[:, q] = np.mean(var, axis=1)    

            S = np.zeros(the_shape)
            for q in range(0, the_shape[-1]):
                S[:, q] = np.std(var, axis=1)    

            try:
                self.scaled_dict[name] = (var - M) / S
            except:
                pass
                #print()
                #print('Whoa')
                #print()

            # so we can undo the transformation later
            self.scaled_dict[name + '_mean'] = M[:, 0:self.config['n_forward']]
            self.scaled_dict[name + '_std'] = S[:, 0:self.config['n_forward']]

            ## scale the y values in the same way as the X values (price) are scaled
            if name == 'X_all_scaled':

                self.scaled_dict['y_all_scaled'] = (self.y_all - M[:, 0:2]) / S[:, 0:2]
                #print(y_all_scaled.shape)
                #sys.exit(0)
                
                self.scaled_dict['y_forward_all_scaled'] = (
                    (self.y_forward_all - M[:, 0:(self.config['n_forward'])]) / S[:, 0:(self.config['n_forward'])]
                )
                    
    #
    # shuffle (optional)
    #
    def shuffle_it(self):
        indices = np.arange(0, self.X_all.shape[0])
        np.random.shuffle(indices)

        self.shuffled_indices = indices

        self.X_all = self.X_all[indices, :]
        self.X_volume_all = self.X_volume_all[indices, :]
        self.X_sin_all = self.X_sin_all[indices, :]
        self.X_cos_all = self.X_cos_all[indices, :]

        # replace this with a for loop

        
        self.y_all = self.y_all[indices]
        self.y_forward_all = self.y_forward_all[indices]
        self.scaled_dict['y_all_scaled'] = self.scaled_dict['y_all_scaled'][indices, :]
    
        self.scaled_dict['X_all_scaled'] = self.scaled_dict['X_all_scaled'][indices, :]
        self.scaled_dict['X_all_scaled_mean'] = self.scaled_dict['X_all_scaled_mean'][indices, :]
        self.scaled_dict['X_all_scaled_std'] = self.scaled_dict['X_all_scaled_std'][indices, :]
    
        self.scaled_dict['X_volume_all_scaled'] = self.scaled_dict['X_volume_all_scaled'][indices, :]
        self.scaled_dict['X_volume_all_scaled_mean'] = self.scaled_dict['X_volume_all_scaled_mean'][indices, :]
        self.scaled_dict['X_volume_all_scaled_std'] = self.scaled_dict['X_volume_all_scaled_std'][indices, :]
    
        self.scaled_dict['MSS_all_scaled'] = self.scaled_dict['MSS_all_scaled'][indices, :]
        self.scaled_dict['MSS_all_scaled_mean'] = self.scaled_dict['MSS_all_scaled_mean'][indices, :]
        self.scaled_dict['MSS_all_scaled_std'] = self.scaled_dict['MSS_all_scaled_std'][indices, :]

    #
    # but we still have to figure out (SOMETHING I FORGOT)
    #
    def assemble_final_structure(self):
        
        # figure out a way to compute this rather than hard code it
        n_features = 5
        
        n_samples = self.X_all.shape[0]
        n_timepoints = self.config['n_back']

        self.M = np.zeros([n_samples, n_timepoints, n_features])

        for i in range(0, n_samples):
            self.M[i, :, 0] = self.scaled_dict['X_all_scaled'][i, :]
            self.M[i, :, 1] = self.scaled_dict['X_volume_all_scaled'][i, :]
            self.M[i, :, 2] = self.scaled_dict['MSS_all_scaled'][i, :]
            self.M[i, :, 3] = self.X_sin_all[i, :]
            self.M[i, :, 4] = self.X_cos_all[i, :]

    #
    # reduce data set size
    #
    # by reducing the number of rows
    #
    def reduce_data_size(self):
        if self.config['reduce_vector_sizes']:
            indices_modulus = np.array([x % self.config['modulus_integer'] for x in range(0, self.M.shape[0])])
            self.indices_modulus_selected = np.where(indices_modulus == 0)[0]
        else:
            self.indices_modulus_selected = np.array(range(0, self.M.shape[0]))

        self.M_after_modulus_operation = self.M[self.indices_modulus_selected, :, :]
        self.y_after_modulus_operation = self.scaled_dict['y_all_scaled'][self.indices_modulus_selected, :]

    #
    # Divide content into training, validation, and test sets
    #
    def get_train_val_test(self):
        self.row_count = self.M_after_modulus_operation.shape[0]

        self.train_val_test_dict = {}
        position = 0
        for group in ['train', 'val', 'test']:
            n = int(self.config['train_val_test_split'][group] * self.row_count)

            self.train_val_test_dict[group] = {
                
                'M' : self.M_after_modulus_operation[position:(position + n), :, :],
                'y' : self.y_after_modulus_operation[position:(position + n), :],
                'n' : n,
                'position' : position,
            }
            
            position += n

    # investigate array lengths after aggregation
    def investigate_array_lengths_after_aggregation(self):
        self.min_array_length = self.arrays_spark_df.select(f.min(f.col('array_length')).alias('min_array_length')).take(1)[0]['min_array_length']        
        self.max_array_length = 1 + self.arrays_spark_df.select(f.max(f.col('array_length')).alias('max_array_length')).take(1)[0]['max_array_length']

        self.arrays_spark_df = (
            self.arrays_spark_df
            .withColumn('min_array_length', f.lit(self.min_array_length))
            .withColumn('max_array_length', f.lit(self.max_array_length))
        )

    
#
# main()
#
if __name__ == '__main__':
    spark = (
        SparkSession
        .builder
        .master('local[*]')
        .appName('badass')
        .config(conf = config['spark_configuration'])
        .getOrCreate()
    )    

    data_prep_root = DataPrep(spark, **config)
