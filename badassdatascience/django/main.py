

import boilerplate

from ec2_spot_price_tracker.models import Ec2SpotPrice



df = Ec2SpotPrice.pull_ec2_data(
    ['Linux/UNIX'],
    'us-west-2a',
    ['m2.xlarge'],
)

Ec2SpotPrice.create_objects(df)

