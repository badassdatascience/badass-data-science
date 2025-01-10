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
        
    s = SolarNoon()
    s.fit()
    s.summary()
    
    date_to_use = datetime.datetime(2024, 1, 1, tzinfo = pytz.utc).date()
    s = SolarNoon(date_to_use_local = date_to_use)
    s.fit()
    s.summary()




