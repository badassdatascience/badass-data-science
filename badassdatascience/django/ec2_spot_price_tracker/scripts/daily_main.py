#
# The following means of connecting to Django
# are crude; there is probably a better way to go
# about this--likely by improving directory structure
# so that the Python path does not need to
# specifically messed with as happens here:
#

#
# How to run it:
#
# python3 $BDS_HOME/badassdatascience/django/ec2_spot_price_tracker/scripts/daily_main.py
#

#
# main
#
if __name__ == '__main__':

    #
    # import useful libraries
    #
    import os
    import sys

    #
    # define paths
    #
    path_bds = os.environ['BDS_HOME']
    path_bds_django = path_bds + '/badassdatascience/django'

    #
    # set paths for Python module search
    #
    sys.path.append(path_bds)
    sys.path.append(path_bds_django)

    #
    # connect to Django
    #
    import boilerplate

    #
    # bring in the model
    #
    from ec2_spot_price_tracker.models import Ec2SpotPrice

    #
    # run an example
    #
    df = Ec2SpotPrice.fetch_and_load_into_database(
        ['Linux/UNIX'],
        'us-west-2a',
        ['m2.xlarge'],
    )

    #
    # Send results to stdout for Nextflow-related testing
    #
    print()
    print(df)
    print()

