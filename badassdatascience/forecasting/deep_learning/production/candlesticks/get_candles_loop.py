#
# useful libraries
#
import json
import pprint as pp
import os
import argparse
import time
import datetime
import sys
import glob


#
# declare command line arguments
#
parser = argparse.ArgumentParser(description='Get Oanda FOREX data.')
parser.add_argument('--config-file', type=str, help='Filename of configuration file.', required=True)
parser.add_argument('--count', type=int, help='Count.', required=True)
parser.add_argument('--granularity', type=str, help='Granularity.', required=True)
parser.add_argument('--output-directory', type=str, help='Output directory.', required=True)
parser.add_argument('--end-date', type=int, help='End date in Unix seconds.')
parser.add_argument('--now', action='store_true', help='Use current time.')
parser.add_argument('--instruments', type=str, help='Comma-delimited list of instruments (with underscores).')
parser.add_argument('--price-types', type=str, help='Just use \'BAM\' and don\'t argue!', required=True)

#
# parse command line arguments
#
command = sys.argv[0]
command_directory = '/'.join(command.split('/')[0:-1])

args = parser.parse_args()

config_file = args.config_file
count = args.count
granularity = args.granularity
output_directory = args.output_directory
end_date = args.end_date
now = args.now
instrument_list = args.instruments.split(',')
price_types = args.price_types

if end_date != None and now:
    print('Cannot use both --end-date and --now. Exiting.')
    sys.exit(0)

if now:
    end_date = int(time.mktime(datetime.datetime.now().timetuple()))

#
# user settings
#
#n = 14 #for hours
n = 1186 #for minutes
download = True
final_file = 'candles_' + str(end_date) + '.json'

#
# figure out where we left off
#
done_list = [x.split('/')[-1] for x in glob.glob(output_directory + '/loop/*.json') if x.find('meta') == -1]

try:
    start_n = max([int(x.split('_')[-1].split('.')[0]) for x in done_list])
except:
    start_n = 0
    
#
# get command
#
def get_command(i, end_date_to_use, output_directory_to_use, config_file_to_use, granularity_to_use, count_to_use, instrument_list_to_use, price_types_to_use, command_directory):

    #return 'python3 ' + command_directory + '/get_candles.py --config-file ' + config_file_to_use + ' --granularity ' + granularity_to_use + ' --count ' + str(count_to_use) + ' --output-file ' + output_directory_to_use + '/loop/candles_' + str(i) + '.json --meta-output-file ' + output_directory_to_use + '/loop/candles_' + str(i) + '_meta.json --instruments ' + ','.join(instrument_list_to_use) + ' --price-types ' + price_types_to_use + ' --end-date ' + str(end_date_to_use)

    return 'python3 get_candles.py --config-file ' + config_file_to_use + ' --granularity ' + granularity_to_use + ' --count ' + str(count_to_use) + ' --output-file ' + output_directory_to_use + '/loop/candles_' + str(i) + '.json --meta-output-file ' + output_directory_to_use + '/loop/candles_' + str(i) + '_meta.json --instruments ' + ','.join(instrument_list_to_use) + ' --price-types ' + price_types_to_use + ' --end-date ' + str(end_date_to_use)

#
# download
#
if download:

    #
    # 
    #
    try:
        os.system(get_command(start_n, end_date, output_directory, config_file, granularity, count, instrument_list, price_types, command_directory))
    except:
        print()
        print('First command failed.')
        print()
        sys.exit(0)
        
    #
    # iterate
    #
    for i in range(start_n, n-1):
        try:
            with open(output_directory + '/loop/candles_' + str(i) + '.json') as f:
                local_candles = json.load(f)

            min_time = end_date
            for instrument in local_candles.keys():
                for item in local_candles[instrument]:
                    test_time = int(float(item['time']))
                    if test_time < min_time:
                        min_time = min(min_time, test_time)

            new_end_date = min_time
            os.system(get_command(i+1, new_end_date, output_directory, config_file, granularity, count, instrument_list, price_types, command_directory))

            time.sleep(2)
        except:
            continue
        
# #
# # assemble
# #
# assembled_candles = {}
# for i in range(n - 1, -1, -1):
#     with open(output_directory + '/loop/candles_' + str(i) + '.json') as f:
#         local_candles = json.load(f)

#     for instrument in local_candles.keys():
#         if not instrument in assembled_candles:
#             assembled_candles[instrument] = []
#         assembled_candles[instrument].extend(local_candles[instrument])

# #
# # verify
# #
# for instrument in assembled_candles.keys():
#     time_list = []
#     for item in assembled_candles[instrument]:
#         time_list.append(int(float(item['time'])))

#     print(time_list == sorted(time_list))
#     print(len(time_list))

# #
# # save
# #
# with open(output_directory + '/' + final_file, 'w') as f:
#     json.dump(assembled_candles, f, indent=2)
