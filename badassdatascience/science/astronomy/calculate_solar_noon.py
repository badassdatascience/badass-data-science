#
# load the libraries we need
#
import ephem
import pytz
import datetime

#
# user settings
#
#latitude = 32.7157
#longitude = -117.1611
#my_timezone = 'America/Los_Angeles'

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
    return noon_dt, (latitude, longitude)

def adjust_timezone_from_UTC(
        datetime_in_UTC,
        to_timezone = 'America/Los_Angeles',
):
    dt_utc_aware = datetime_in_UTC.replace(tzinfo = pytz.utc)
    dt_tz = dt_utc_aware.astimezone(pytz.timezone(to_timezone))
    return dt_tz, to_timezone

#def convert_datetime_HMS_to_hours_in_decimal(datetime_hms):
#    hour = datetime_hms.hour
#    minute = datetime_hms.minute
#    second = datetime_hms.second
#
#    seconds_in_minutes = second / 60.
#    hour_as_decimal = hour + (minute / 60.) + (seconds_in_minutes / 60.)
#    return hour_as_decimal

noon_utc, (lat, lon) = calculate_solar_noon_in_UTC()
noon_local, timezone = adjust_timezone_from_UTC(noon_utc)



print(noon_utc, lat, lon)
print(noon_local, timezone)

#print('Today\'s solar noon occurs at:', str(noon_local).split(' ')[1])

