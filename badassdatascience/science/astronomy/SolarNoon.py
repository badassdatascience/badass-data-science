#
# load the libraries we need
#
import ephem
import pytz
import datetime
import json
import pprint as pp

class SolarNoon():

    #
    # Constructor
    #
    def __init__(
            self,

            #
            # Defaults to San Diego, California, USA
            #
            latitude = 32.7157,
            longitude = -117.1611,
            local_timezone = 'America/Los_Angeles',

            #
            # the date you want a computation for, in local timezone
            #
            date_to_use_local = None,
    ):
        #
        # initialize object parameters
        #
        self.latitude = latitude
        self.longitude = longitude

        #
        # this is the timezone you want solar noon to be reported in
        # and the input date (if given) to use
        #
        self.local_timezone = local_timezone

        #
        # right now, I've only implemented this to take a date in UTC
        # if a date is given, a matter that should be adjusted
        #
        self.date_to_use_local = date_to_use_local

    #
    # Once an object is defined, use this to compute the result
    #
    # The workflow style here is based on scikit-learn
    # where one often declares a learning object
    # (with parameters), and then "fits" it to the data
    # in a separate, subsequent command
    #
    def fit(self):
        self.calculate_todays_solar_noon()
        self.adjust_timezone_from_UTC()

    #
    # computes solar noon
    #
    def calculate_todays_solar_noon(self):

        #
        # declare pyephem objects
        #
        observer = ephem.Observer()
        observer.lat, observer.lon = str(self.latitude), str(self.longitude)
        sun = ephem.Sun()

        #
        # defaults to today in the given local timezone unless a local date
        # is given to the constructor ("date_to_use_local")
        #
        self.start = datetime.datetime.now().astimezone(pytz.timezone(self.local_timezone)).date()
        if self.date_to_use_local != None:
            self.start = self.date_to_use_local
            
        #
        # computations in UTC
        #
        sunrise = observer.previous_rising(sun, start = self.start)
        self.noon_UTC_pyephem = observer.next_transit(sun, start = sunrise)

        #
        # convert to a UTC-aware datetime object
        #
        self.noon_UTC_datetime = self.noon_UTC_pyephem.datetime().replace(tzinfo = pytz.utc)

    #
    # convert computed time to the local timezone defined
    # in the constructor
    #
    def adjust_timezone_from_UTC(self):
        self.noon_local_datetime = self.noon_UTC_datetime.astimezone(pytz.timezone(self.local_timezone))

    #
    # create a fancy printout, maybe in JSON
    #
    def summary(self):
        output = {
            'Date' : str(self.start),
            'Latitude' : self.latitude,
            'Longitude' : self.longitude,
            'Timezone' : self.local_timezone,
            'Solar Noon' : str(self.noon_local_datetime).split(' ')[1].split('.')[0],
        }

        self.output_json = json.dumps(output)
        pp.pprint(output)

#
# test drive
#
if __name__ == "__main__":

    import pandas as pd
    import matplotlib.pyplot as plt

    from badassdatascience.utilities.badass_datetime.badass_datetime import convert_datetime_hms_to_hour_decimal
    
    #
    # individual test cases
    #
    s = SolarNoon()
    s.fit()
    print()
    s.summary()
    
    date_to_use = datetime.datetime(2024, 1, 1, tzinfo = pytz.utc).date()
    s = SolarNoon(date_to_use_local = date_to_use)
    s.fit()
    s.summary()

    s = SolarNoon(
        date_to_use_local = date_to_use,
        local_timezone = 'UTC',
    )
    s.fit()
    s.summary()

    #
    # computation for a whole year
    #
    date_list = [
        datetime.date(2024, 1, 1) + datetime.timedelta(days = x)
        for x in range(0, 365 + 1)
    ]
    df = pd.DataFrame({'date' : pd.to_datetime(date_list)})
    
    def apply_solar_noon(date_item):
        s = SolarNoon(date_to_use_local = date_item)
        s.fit()
        noon = s.noon_local_datetime
        noon_decimal = convert_datetime_hms_to_hour_decimal(noon)
        return noon_decimal

    df['solar_noon'] = df['date'].apply(apply_solar_noon)

    print()
    print(df)
    print()
    
