#
# load useful libraries
#
import boto3
import pickle
import pandas as pd
import numpy as np
import time
import datetime
import matplotlib.pyplot as plt

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

            #
            # What kind of ec2 spot price do we want?
            #
            product_description = 'Linux/UNIX',
            availability_zone = 'us-west-2a',
            instance_type = 'm2.xlarge',

            #
            # These two variables handle pagination;
            # there is probably a better way to handle pagination
            #
            time_to_sleep_between_post_requests = 5,
            number_of_times_to_run_post_requests = 5,

            #
            # Pandas resampling frequency expressed as "'X'min";
            # "1440min" is a day:
            #
            frequency_str = '1440min',

            output_directory = None,
    ):

        self.product_description = product_description
        self.availability_zone = availability_zone
        self.instance_type = instance_type
        self.time_to_sleep_between_post_requests = time_to_sleep_between_post_requests
        self.number_of_times_to_run_post_requests = number_of_times_to_run_post_requests
        self.frequency_str = frequency_str

        self.output_directory = output_directory

        #
        # save the initiation time
        #
        self.initiation_timestamp_str = str(datetime.datetime.utcnow()).replace(' ', '--')
        
        
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
            AvailabilityZone = self.availability_zone,
            ProductDescriptions = [self.product_description],
            InstanceTypes = [self.instance_type],
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
        for i in range(0, self.number_of_times_to_run_post_requests):
            time.sleep(self.time_to_sleep_between_post_requests)
            response = ec2.describe_spot_price_history(
                NextToken = response['NextToken'],
                AvailabilityZone = self.availability_zone,
                ProductDescriptions = [self.product_description],
                InstanceTypes = [self.instance_type],
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
        #
        def custom_resampler(arraylike):
            return np.mean([float(x) for x in arraylike])

        self.df_pre_resample.index = self.df_pre_resample['Timestamp']

        self.series = (
            self.df_pre_resample['SpotPrice']
            .resample(self.frequency_str)
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
        with open(self.output_directory + '/' + self.initiation_timestamp_str + '.pickle', 'wb') as f:
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
                self.availability_zone,
                self.instance_type,
                self.product_description,
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
            plt.savefig(self.output_directory + '/' + self.initiation_timestamp_str + '.png')
        
        plt.show()
        plt.close()
        
