

#
# import useful libraries
#
import os
import sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#
# user settings
# Crude--Should be command line arguments or taken from a configuration
# file:
#
query_filters = {
    'product_description' : 'Linux/UNIX',
    'availability_zone' : 'us-west-2a',
    'instance_type' : 'm2.xlarge',
}

#
# connect to database using Django
#
path_bds = os.environ['BDS_HOME']
path_bds_django = path_bds + '/badassdatascience/django' # crude

sys.path.append(path_bds)
sys.path.append(path_bds_django)

import boilerplate

#
# load the model definition
#
from ec2_spot_price_tracker.models import Ec2SpotPrice

# Get the objects that match the search criteria
queryset = Ec2SpotPrice.objects.filter(**query_filters)

# Convert the queryset to a list of dictionaries
data = list(queryset.values())

# Create a DataFrame from the list of dictionaries
df = pd.DataFrame(data)

#
# define a resampling function
#
def resample(df, frequency_str = '1440min'):

    #
    # We need to define a custom resampler to compute the
    # mean value over the interval period

    def custom_resampler(arraylike):
        return np.mean([float(x) for x in arraylike])

    df_pre_resample = df.copy()
    df_pre_resample.index = df_pre_resample['timestamp']
    
    series = (
        df_pre_resample['spot_price']
        .resample(frequency_str)
        .apply(custom_resampler)
    )

    df_post_resample = (
        pd
        .DataFrame(
            {
                'spot_price' : series
            },
            index=series.index
        )
        .sort_index()
    )

    return df_post_resample

#
# resample
#
df_post_resample = resample(df)

#
# generate some stdout content useful for debugging Nextflow
#
print()
print(df_post_resample)
print()




plt.figure()
plt.plot(df_post_resample.index, df_post_resample['spot_price'], '.-')
plt.xticks(rotation = 80)
plt.tight_layout()
plt.savefig('output/test.png')
plt.close()
