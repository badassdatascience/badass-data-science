"""
Given the output of the following:

python3 $APP_HOME/infrastructure/r_and_d/model_building/get_candles_loop.py --config-file ../../data/DEVELOPMENT.json --count 5000 --granularity H1 --output-directory output --now --price-types BAM --instruments EUR_USD,USD_CAD,USD_JPY,GBP_USD,AUD_USD,USD_CHF,NZD_USD

...loads its results into the database.
"""

#
# load system libraries
#
import json
import pprint as pp
import glob
import os
import argparse

#
# import local libraries
#
import boilerplate
import library.database as db

#
# declare command line arguments
#
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
