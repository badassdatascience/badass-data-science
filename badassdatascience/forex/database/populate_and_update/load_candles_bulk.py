"""
Given the output of the following:

python3 $BDS_HOME/badassdatascience/forex/database/populate_and_update/get_candles_loop.py --config-file $BDS_HOME/badassdatascience/forex/data/DEVELOPMENT.json --count 5000 --granularity D --output-directory output --now --price-types BAM --instruments EUR_USD

...loads its results into the database.

To run:

cd $BDS_HOME

python3 badassdatascience/forex/database/populate_and_update/load_candles_bulk.py --source-directory badassdatascience/forex/output/loop --interval-name Day

"""

#
# load system libraries
#
import json
import pprint as pp
import glob
import os
import sys
import argparse

#
# configure relevant directory access
#
root_directory_application = os.environ['BDS_HOME'] + '/badassdatascience/forex'
root_directory_django = os.environ['BDS_HOME'] + '/badassdatascience/django'

#data_directory = root_directory_application + '/data'
#price_types_file = data_directory + '/price_types.json'
#intervals_file = data_directory + '/intervals.json'
#instruments_file = data_directory + '/instruments_and_margin_requirements.csv'

#
# set the path and environment
#
sys.path.append(os.environ['BDS_HOME'])
sys.path.append(root_directory_application)
sys.path.append(root_directory_django)

os.environ['DJANGO_SETTINGS_MODULE'] = 'infrastructure.settings'

#
# load Django 
#
import django
django.setup()
from django.utils import timezone

#
# import local libraries
#
import library.database as db

#
# declare command line arguments
#
"""
python3 load_candles_bulk.py --source-directory ../candlesticks/output/loop --interval-name Minute
"""

parser = argparse.ArgumentParser(description='Load initial candles into database.')
parser.add_argument('--source-directory', type=str, help='Source directory.', required=True)
parser.add_argument('--interval-name', type=str, help='e.g. "Hour"', required=True)

#
# user settings
#
args = parser.parse_args()
source_directory = args.source_directory
file_template = 'candles_*.json'
interval_name = args.interval_name

#
# load the rest of the necessary libraries
#
from timeseries.models import Interval

#
# get files and remove "meta"
#
filelist = [x for x in glob.glob(source_directory + '/' + file_template) if x.find('meta') < 0]

#
# iterate through the files to get distinct price types, instruments, timestamps
#
instrument_dict, price_type_dict, timestamp_dict, volume_dict = db.get_distinct_content(filelist)
                        
#
# get instrument objects from database
#
instrument_dict = db.populate_instrument_dictionary(instrument_dict)
        
#
# get price type objects from database
#
price_type_dict = db.populate_price_type_dictionary(price_type_dict)

#
# load timestamps into database
#
success = db.load_timestamps_into_database(timestamp_dict)

#
# load volumes into database
#
success = db.load_volumes_into_database(volume_dict)
        
#
# load interval object from database
#
interval  = Interval.objects.get(name=interval_name) 

#
# iterate through the files to populate the candlestick object
#
success = db.load_candlesticks(filelist, interval, instrument_dict, timestamp_dict, volume_dict, price_type_dict)
