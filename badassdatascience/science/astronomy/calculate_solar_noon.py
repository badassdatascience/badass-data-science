#
# load the libraries we need
#
import ephem
import pytz
import datetime

#
# user settings
#
latitude = 33.1192
longitude = -117.0864
my_timezone = 'America/Los_Angeles'

#
# calculate solar noon in UTC
#
def calculate_solar_noon_in_UTC(

        # San Diego, California, USA
        latitude = 32.7157,
        longitude = -117.1611,

        utc_date = None
):
    observer = ephem.Observer()
    observer.lat, observer.long = str(latitude), str(longitude)
    sun = ephem.Sun()

    start = ephem.now()
    if utc_date != None:
        start = utc_date

    
    sunrise = observer.previous_rising(sun, start = start)
    noon = observer.next_transit(sun, start = sunrise)
    noon_dt = noon.datetime()
    return noon_dt

def adjust_timezone_from_UTC(
        datetime_in_UTC,
        to_timezone = 'America/Los_Angeles',
):
    dt_utc_aware = datetime_in_UTC.replace(tzinfo = pytz.utc)
    dt_tz = dt_utc_aware.astimezone(pytz.timezone(to_timezone))
    return dt_tz

def convert_datetime_HMS_to_hours_in_decimal(datetime_hms):
    hour = datetime_hms.hour
    minute = datetime_hms.minute
    second = datetime_hms.second

    seconds_in_minutes = second / 60.
    hour_as_decimal = hour + (minute / 60.) + (seconds_in_minutes / 60.)
    return hour_as_decimal



noon_UTC = calculate_solar_noon_in_UTC()
noon_local = adjust_timezone_from_UTC(noon_UTC)

start_date = datetime.date(2024, 1, 1)
date_list = [start_date + datetime.timedelta(days = x) for x in range(0, 365 + 1)]

solar_noon_HMS_list = [
    adjust_timezone_from_UTC(calculate_solar_noon_in_UTC(utc_date = x)) for x in date_list
]



solar_noon_decimal_list = [convert_datetime_HMS_to_hours_in_decimal(x) for x in solar_noon_HMS_list]

print(solar_noon_decimal_list[0:5])



