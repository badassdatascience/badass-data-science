#
# import system libraries
#
import sys
import os
import json

#
# user settings
#
root_directory_application = os.environ['BDS_HOME'] + '/badassdatascience/forecasting/deep_learning'
root_directory_django = os.environ['BDS_HOME'] + '/badassdatascience/django'

data_directory = root_directory_application + '/data'
price_types_file = data_directory + '/price_types.json'
intervals_file = data_directory + '/intervals.json'
instruments_file = data_directory + '/instruments_and_margin_requirements.csv'

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
# load the rest of the necessary libraries
#
from timeseries.models import PriceType, Instrument, Interval

#
# clear the data
#
Interval.objects.all().delete()
PriceType.objects.all().delete()
Instrument.objects.all().delete()

#
# load intervals
#
with open(intervals_file) as f:
    intervals = json.load(f)
for entry in intervals['intervals']:
    iv = Interval(
        name = entry['name'],
        symbol = entry['symbol'],
        pub_date=timezone.now()
    )
    iv.save()

#
# enter the price types into the database
#
with open(price_types_file) as f:
    price_types = json.load(f)

for name in price_types['price_types']:
    pt = PriceType(name=name, pub_date=timezone.now())
    pt.save()
    
#
# enter instruments
#
f = open(instruments_file)
for line in f:
    line = [x.strip() for x in line.strip().split(',')]
    name = line[0]
    if name == 'instrument':
        continue
    margin_requirement = float(line[1]) / 100.
    instrument = Instrument(
        name = name,
        us_margin_requirement = margin_requirement,
        pub_date = timezone.now()
    )
    instrument.save()
f.close()
