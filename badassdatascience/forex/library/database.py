#
# load necessary libraries
#
import json
from django.utils import timezone
from django.db import transaction

from timeseries.models import PriceType, Instrument, Candlestick, Timestamp, Volume

#
# iterate through the files to get distinct price types, instruments, timestamps
#
def get_distinct_content(filelist):
    instrument_dict = {}
    price_type_dict = {}
    timestamp_dict = {}
    volume_dict = {}
    for filename in filelist:
        with open(filename) as f:
            contents = json.load(f)
        for instrument in contents.keys():
            instrument_dict[instrument.replace('_', '/')] = None
            for entry in contents[instrument]:
                timestamp = int(float(entry['time']))
                timestamp_dict[timestamp] = None
                volume = entry['volume']
                volume_dict[volume] = None
                for pt in ['ask', 'bid', 'mid']:
                    if pt in entry:
                        price_type_dict[pt] = None
    return instrument_dict, price_type_dict, timestamp_dict, volume_dict

#
# get instrument objects from database
#
def populate_instrument_dictionary(instrument_dict):
    new_instrument_dict = {}
    for instrument in instrument_dict.keys():
        new_instrument_dict[instrument] = Instrument.objects.get(name=instrument.replace('_', '/'))
    return new_instrument_dict

#
# get price type objects from database
#
def populate_price_type_dictionary(price_type_dict):
    new_price_type_dict = {}
    for price_type in price_type_dict.keys():
        new_price_type_dict[price_type] = PriceType.objects.get(name=price_type)
    return new_price_type_dict

#
# load timestamps into database
#
def load_timestamps_into_database(timestamp_dict):
    timestamp_list = sorted(list(timestamp_dict.keys()))
    with transaction.atomic():
        for ts in timestamp_list:
            timestamp_dict[ts], created = Timestamp.objects.get_or_create(
                timestamp = ts,
                defaults = {'pub_date' : timezone.now()},
            )
    return True

#
# load volumes into database
#
def load_volumes_into_database(volume_dict):
    volume_list = sorted(list(volume_dict.keys()))
    with transaction.atomic():
        for volume in volume_list:
            volume_dict[volume], created = Volume.objects.get_or_create(
                volume = volume,
                defaults = {'pub_date' : timezone.now()},
                )
    return True

#
# iterate through the files to populate the candlestick object
#
def load_candlesticks(filelist, interval, instrument_dict, timestamp_dict, volume_dict, price_type_dict):
    with transaction.atomic():
        for filename in filelist:
            with open(filename) as f:
                contents = json.load(f)
            for instrument in contents.keys():
                instrument_object = instrument_dict[instrument.replace('_', '/')]
                for entry in contents[instrument]:
                    if not entry['complete']:
                        continue
                    timestamp = timestamp_dict[int(float(entry['time']))]
                    volume = volume_dict[entry['volume']]
                    for pt in ['ask', 'bid', 'mid']:
                        price_type = price_type_dict[pt]
                        o = float(entry[pt]['o'])
                        h = float(entry[pt]['h'])
                        l = float(entry[pt]['l'])
                        c = float(entry[pt]['c'])
                        candlestick, created = Candlestick.objects.get_or_create(
                            o = o,
                            h = h,
                            l = l,
                            c = c,
                            volume = volume,
                            interval = interval,
                            price_type = price_type,
                            timestamp = timestamp,
                            instrument = instrument_object,
                            defaults = {'pub_date' : timezone.now()},
                            )
    return True
