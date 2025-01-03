

import boilerplate

from ec2_spot_price_tracker.models import Ec2SpotPrice



df = Ec2SpotPrice.fetch_and_load_into_database(
    ['Linux/UNIX'],
    'us-west-2a',
    ['m2.xlarge'],
)


