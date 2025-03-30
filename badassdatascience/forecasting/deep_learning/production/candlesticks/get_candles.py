#
# libraries
#
import pprint as pp
import json
import requests
import datetime
import time
import argparse
import sys

#
# declare command line arguments
#
parser = argparse.ArgumentParser(description='Get Oanda FOREX data.')
parser.add_argument('--config-file', type=str, help='Filename of configuration file.', required=True)
parser.add_argument('--count', type=int, help='Count.', required=True)
parser.add_argument('--granularity', type=str, help='Granularity.', required=True)
parser.add_argument('--output-file', type=str, help='Output filename.', required=True)
parser.add_argument('--meta-output-file', type=str, help='Meta data output filename.', required=True)
parser.add_argument('--end-date', type=int, help='End date in Unix seconds.')
parser.add_argument('--now', action='store_true', help='Use current time.')
parser.add_argument('--instruments', type=str, help='Comma-delimited list of instruments (with underscores).')
parser.add_argument('--price-types', type=str, help='Just use \'BAM\' and don\'t argue!', required=True)

#
# parse command line arguments
#
args = parser.parse_args()

config_file = args.config_file
count = args.count
granularity = args.granularity
output_file = args.output_file
output_file_meta = args.meta_output_file
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
# load config
#
with open(config_file) as f:
    config = json.load(f)

#
# specify headers
#
headers = {
    'Content-Type' : 'application/json',
    'Authorization' : 'Bearer ' + config['token'],
    'Accept-Datetime-Format' : config['oanda_date_time_format'],
}

#
# save the data
#
data = {}
for instrument in instrument_list:
    url = config['server'] + '/v3/instruments/' + instrument + '/candles?count=' + str(count) + '&price=' + price_types + '&granularity=' + granularity + '&to=' + str(end_date)

    got_it = False
    while not got_it:
        try:
            r = requests.get(url, headers=headers)
            data[instrument] = r.json()['candles']
            got_it = True
        except:
            time.sleep(5)
            continue

        
with open(output_file, 'w') as f:
    json.dump(data, f, indent=4)

#
# save candles meta data
#
meta = {
    'end_date' : end_date,
    }
with open(output_file_meta, 'w') as f:
    json.dump(meta, f, indent=4)
