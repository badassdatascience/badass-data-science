from django.db import models

import boto3
import pandas as pd
import numpy as np
import time
import datetime

class Ec2SpotPrice(models.Model):

    #
    # Data model definition
    #
    spot_price = models.FloatField(null = False)
    timestamp = models.DateTimeField('Timestamp for this entry')
    product_description = models.CharField(max_length = 200, null = False)
    availability_zone = models.CharField(max_length = 200, null = False)
    instance_type = models.CharField(max_length = 200, null = False)
    pub_date = models.DateTimeField('Date loaded into database')

    #
    # create objects
    #
    def create_objects(df):
        for i, row in df.iterrows():
            ec2_spot_price_instance = Ec2SpotPrice(
                spot_price = row['SpotPrice'],
                timestamp = row['Timestamp'],
                product_description = row['ProductDescription'],
                availability_zone = row['AvailabilityZone'],
                instance_type = row['InstanceType'],
                pub_date = datetime.datetime.now(),  # there is a better way for this
            )
            ec2_spot_price_instance.save()
            
    
    #
    # Pull ec2 spot price data from AWS and save instances
    #
    def pull_ec2_data(
        product_description,
        availability_zone,
        instance_type,

        time_to_sleep_between_post_requests = 5,
        number_of_times_to_run_post_requests = 5,
    ):

        ec2 = boto3.client('ec2')
        
        spot_price_list = []

        response = ec2.describe_spot_price_history(
            AvailabilityZone = availability_zone,
            ProductDescriptions = product_description,
            InstanceTypes = instance_type,
        )
        spot_price_list.extend(response['SpotPriceHistory'])

        df = pd.DataFrame(spot_price_list)
        timestamp_min = np.min(df['Timestamp'])

        #
        # The variables "number_of_times_to_run_post_requests"
        # and "time_to_sleep_between_post_requests" handle
        # pagination.
        #
        # There is probably a better way to handle pagination
        #
        for i in range(0, number_of_times_to_run_post_requests):
            time.sleep(time_to_sleep_between_post_requests)
            response = ec2.describe_spot_price_history(
                NextToken = response['NextToken'],
                AvailabilityZone = availability_zone,
                ProductDescriptions = product_description,
                InstanceTypes = instance_type,
                EndTime = timestamp_min,
                StartTime = timestamp_min - datetime.timedelta(weeks=6)
            )

            spot_price_list.extend(response['SpotPriceHistory'])
            df = pd.DataFrame(spot_price_list)
            timestamp_min = np.min(df['Timestamp'])

        return df

        

