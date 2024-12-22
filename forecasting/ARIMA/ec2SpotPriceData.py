#
# load useful libraries
#
import boto3
import pickle
import json
import pandas as pd
import numpy as np
import time
import datetime
import matplotlib.pyplot as plt
import uuid

#
# Define class for retrieving ec2 spot price data from AWS
# and aggregating it
#
class ec2SpotPriceData():

    #
    # Constructor
    #
    def __init__(
            self,
            configuration_json_filename,
    ):

        with open(configuration_json_filename) as f:
            self.config = json.load(f)

        self.configuration_json_filename = configuration_json_filename
            
        #
        # save the initiation time
        #
        self.config['initiation_timestamp_str'] = str(time.time())

        #
        # create a unique ID for each instance
        #
        self.config['uuid'] = str(uuid.uuid4()) + str(uuid.uuid4())
        
    #
    # The "fit" language, which might not seem natural
    # for a data pull operation, simply follows the
    # scikit-learn language pattern
    #
    def fit(self):
        self.pull_ec2_spot_price_data_from_AWS()
        self.assemble_and_clean_the_pull_data()
        self.resample()
        self.create_final_dataframe()
        
    #
    # Pull ec2 spot price data from AWS
    #
    def pull_ec2_spot_price_data_from_AWS(self):

        ec2 = boto3.client('ec2')
        
        spot_price_list = []
        self.df_pre_resample_list = []

        response = ec2.describe_spot_price_history(
            AvailabilityZone = self.config['availability_zone'],
            ProductDescriptions = [self.config['product_description']],
            InstanceTypes = [self.config['instance_type']],
        )
        spot_price_list.extend(response['SpotPriceHistory'])

        df_pre_resample_portion = pd.DataFrame(spot_price_list)
        self.df_pre_resample_list.append(df_pre_resample_portion)
        timestamp_min = np.min(df_pre_resample_portion['Timestamp'])

        #
        # The variables "self.number_of_times_to_run_post_requests"
        # and "self.time_to_sleep_between_post_requests" handle
        # pagination.
        #
        # There is probably a better way to handle pagination
        #
        for i in range(0, self.config['number_of_times_to_run_post_requests']):
            time.sleep(self.config['time_to_sleep_between_post_requests'])
            response = ec2.describe_spot_price_history(
                NextToken = response['NextToken'],
                AvailabilityZone = self.config['availability_zone'],
                ProductDescriptions = [self.config['product_description']],
                InstanceTypes = [self.config['instance_type']],
                EndTime = timestamp_min,
                StartTime = timestamp_min - datetime.timedelta(weeks=6)
            )

            spot_price_list.extend(response['SpotPriceHistory'])
            df_pre_resample_portion = pd.DataFrame(spot_price_list)
            self.df_pre_resample_list.append(df_pre_resample_portion)
            timestamp_min = np.min(df_pre_resample_portion['Timestamp'])

    #
    # Assemble the queried data into one dataframe
    #
    def assemble_and_clean_the_pull_data(self):
        self.df_pre_resample = (
            pd.concat(
                self.df_pre_resample_list
            )
            .drop_duplicates()
            .sort_values(
                by = ['Timestamp']
            )
            .reset_index()
            .drop(
                columns = ['index']
            )
        )

    #
    # Resample
    #
    def resample(self):

        #
        # We need to define a custom resampler to compute the
        # mean value over the interval period

        def custom_resampler(arraylike):
            return np.mean([float(x) for x in arraylike])

        self.df_pre_resample.index = self.df_pre_resample['Timestamp']

        self.series = (
            self.df_pre_resample['SpotPrice']
            .resample(self.config['frequency_str'])
            .apply(custom_resampler)
        )

    #
    # Create final dataframe
    #
    def create_final_dataframe(self):
        self.df = pd.DataFrame({'spot_price' : self.series})

    #
    # Save
    #
    def save(self):
        
        with open(self.config['output_filename_root_directory'] + '/ec2_instance' + '__' + self.config['uuid'] + '__' + self.config['initiation_timestamp_str'] + '.pickle', 'wb') as f:
            pickle.dump(self, f)
            
    #
    # Plot
    #
    def plot(
            self,
            title = None,
            xlabel = 'Date',
            ylabel = 'Spot Price',
            save = False,
    ):

        if title == None:
            title = "Mean ec2 spot prices per day\n%s - %s - %s" % (
                self.config['availability_zone'],
                self.config['instance_type'],
                self.config['product_description'],
            )
        
        timestamp = self.df.index
        spot_price = self.df['spot_price']
        plt.figure()
        plt.plot(timestamp, spot_price, '.-', label = 'Mean Spot Price per Day')
        plt.legend()
        plt.title(title)
        plt.xticks(rotation = 85)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.tight_layout()

        if save:
            plt.savefig(self.config['output_filename_root_directory'] + '/ec2_spot_price__' + self.config['uuid'] + '__' + self.config['initiation_timestamp_str'] + '.png')
        
        plt.show()
        plt.close()

