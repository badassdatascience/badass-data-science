#
# Import useful modules
#
from django.db import models
from django.utils import timezone

import boto3
import pandas as pd
import numpy as np
import time
import datetime

#
# Define a model for describing AWS EC2 spot prices
#
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
    # Enforce uniqueness
    #
    class Meta:
        db_table = 'ec2_spot_price_tracker_ec2_spot_price',
        constraints = [
            models.UniqueConstraint(
                name = 'unique_timestamp_product_description_availability_zone_instance_type',
                fields = [
                    'timestamp',
                    'product_description',
                    'availability_zone',
                    'instance_type',
                ],
            )
        ]

    #
    # Create instances of this class and 
    # store them in the database
    #
    def create_objects(df):
        for i, row in df.iterrows():
            ec2_spot_price_instance, created = Ec2SpotPrice.objects.update_or_create(
                timestamp = row['Timestamp'],
                product_description = row['ProductDescription'],
                availability_zone = row['AvailabilityZone'],
                instance_type = row['InstanceType'],

                defaults = {
                    'spot_price' : row['SpotPrice'],
                    'pub_date' : timezone.now(),  # there is a better way for this
                },
            )
    
    #
    # Pull ec2 spot price information from AWS
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

    #
    # Entry point
    #
    def fetch_and_load_into_database(
        product_description,
        availability_zone,
        instance_type,
        time_to_sleep_between_post_requests = 5,
        number_of_times_to_run_post_requests = 5,
    ):

        df = Ec2SpotPrice.pull_ec2_data(
            product_description,
            availability_zone,
            instance_type,
            time_to_sleep_between_post_requests = time_to_sleep_between_post_requests,
            number_of_times_to_run_post_requests = number_of_times_to_run_post_requests,
        )
        
        Ec2SpotPrice.create_objects(df)
        return df
        

