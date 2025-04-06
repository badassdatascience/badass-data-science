
# python3 get_candles.py --config-file /home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forex/data/DEVELOPMENT.json --count 5000 --granularity D --output-file output_temp/booger.json --meta-output-file output_temp/booger_meta.json --now --instruments "EUR_USD,USD_CAD,USD_JPY,USD_CHF,AUD_USD,GBP_USD,NZD_USD" --price-types BAM

#
# libraries
#
import json
import requests
import datetime
import time
import argparse
import pytz

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
# define fixed values
#
timezone = pytz.timezone('America/Toronto')

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
    end_date_original = int(time.mktime(datetime.datetime.now().timetuple()))
    end_date = end_date_original

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
# iterate through the instruments
#
for instrument in instrument_list:

    #
    # initialize per instrument
    #
    finished = False
    end_date = end_date_original

    #
    # loop through the timestamp ranges for each set of n=count values
    #
    while not finished:

        #
        # retrieve the instrument candlesticks from the Oanda server
        #
        url = config['server'] + '/v3/instruments/' + instrument + '/candles?count=' + str(count) + '&price=' + price_types + '&granularity=' + granularity + '&to=' + str(end_date)
        r = requests.get(url, headers=headers)
        rj = r.json()
        candlesticks = rj['candles']

        #
        # deal with timestamps and time-related content
        #
        date_list = []
        for candle in candlesticks:
            candle['time'] = int(float(candle['time']))
            time_dt = datetime.datetime.fromtimestamp(candle['time'], tz = timezone)
            candle['time_str'] = str(time_dt)
            candle['weekday'] = time_dt.weekday()
            candle['hour'] = time_dt.hour
            date_list.append(candle['time'])

        rj['timestamp_int_min'] = min(date_list)
        rj['timestamp_int_max'] = max(date_list)

        #
        # Are we done with the current instrument?
        #
        if len(date_list) < count:
            finished = True
        
        #
        # prepare for the next iteration
        #
        end_date = rj['timestamp_int_min'] - 0.1
        



        import pprint as pp; pp.pprint(rj)

