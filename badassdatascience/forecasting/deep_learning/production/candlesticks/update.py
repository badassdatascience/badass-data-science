#
# import useful libraries
#
import uuid
import time
import os
import datetime
import argparse

import boilerplate

#
# declare command line arguments
#
parser = argparse.ArgumentParser(description='Get Oanda FOREX data.')
parser.add_argument('--config-file', type=str, help='Filename of configuration file.', required=True)
parser.add_argument('--count', type=int, help='Count.', required=True)
parser.add_argument('--granularity', type=str, help='Granularity.', required=True)
parser.add_argument('--output-directory', type=str, help='Output directory.', required=True)
parser.add_argument('--command-directory', type=str, help='Command directory.', required=True)
parser.add_argument('--instruments', type=str, help='Comma-delimited list of instruments (with underscores).')
parser.add_argument('--price-types', type=str, help='Just use \'BAM\' and don\'t argue!', required=True)

#
# parse command line arguments
#
args = parser.parse_args()
command_directory = args.command_directory
output_directory = args.output_directory
config_file = args.config_file
count = args.count
instrument_list = args.instruments.split(',')
price_types = args.price_types
granularity = args.granularity

#
# get a unique directory ID
#
output_directory = output_directory + '/' + str(uuid.uuid4())
os.mkdir(output_directory)

#
# calculate "now"
#
end_date = int(time.mktime(datetime.datetime.now().timetuple()))

#
# function to get command
#
def get_command(end_date_to_use, output_directory_to_use, config_file_to_use, granularity_to_use, count_to_use, instrument_list_to_use, price_types_to_use, command_directory):
    return 'python3 ' + command_directory + '/get_candles.py --config-file ' + config_file_to_use + ' --granularity ' + granularity_to_use + ' --count ' + str(count_to_use) + ' --output-file ' + output_directory_to_use + '/candles.json --meta-output-file ' + output_directory_to_use + '/candles_meta.json --instruments ' + ','.join(instrument_list_to_use) + ' --price-types ' + price_types_to_use + ' --end-date ' + str(end_date_to_use)

#
# get command
#
cmd = get_command(
    end_date,
    output_directory,
    config_file,
    granularity,
    count,
    instrument_list,
    price_types,
    command_directory,
)

#
# get candles
#
os.system(cmd)

#
# function to load file
#
def load_candles(candles_file, granularity):

    #
    # load necessary libraries
    #
    import json
    import boilerplate
    import library.database as db
    from timeseries.models import Interval

    #
    # iterate through the files to get distinct price types, instruments, timestamps
    #
    instrument_dict, price_type_dict, timestamp_dict, volume_dict = db.get_distinct_content([candles_file])

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
    interval  = Interval.objects.get(symbol=granularity)
    
    #
    # iterate through the files to populate the candlestick object
    #
    success = db.load_candlesticks([candles_file], interval, instrument_dict, timestamp_dict, volume_dict, price_type_dict)
    
#
# load
#
load_candles(output_directory + '/candles.json', granularity)
    
#
# clean up
#
os.remove(output_directory + '/candles.json')
os.remove(output_directory + '/candles_meta.json')
os.rmdir(output_directory)
