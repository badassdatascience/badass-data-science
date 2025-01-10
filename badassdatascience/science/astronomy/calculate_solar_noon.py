#
# load the libraries we need
#
import ephem
import pytz
from datetime import datetime

#
# user settings
#
localFormat = '%Y-%m-%d %H:%M:%S'
latitude = 33.1192
longitude = -117.0864
my_timezone = 'America/Los_Angeles'

#
# calculate solar noon in UTC
#
def calculate_solar_noon_in_UTC(latitude, longitude):
    observer = ephem.Observer()
    observer.lat, observer.long = str(latitude), str(longitude)
    sun = ephem.Sun()
    sunrise = observer.previous_rising(sun) #, start = ephem.now())
    noon = observer.next_transit(sun, start = sunrise)
    noon_dt = noon.datetime()
    return noon_dt

def adjust_timezone(the_datetime_in_UTC, the_timezone = 'America/Los_Angeles'):
    dt_utc_aware = the_datetime_in_UTC.replace(tzinfo = pytz.utc)
    dt_tz = dt_utc_aware.astimezone(pytz.timezone(the_timezone))
    return dt_tz

noon_UTC = calculate_solar_noon_in_UTC(latitude, longitude)
noon_local = adjust_timezone(noon_UTC)
