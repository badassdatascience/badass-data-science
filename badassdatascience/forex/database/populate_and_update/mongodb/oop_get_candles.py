import json
import requests
import datetime
import time
import pytz
import pandas as pd
import matplotlib.pyplot as plt

from badassdatascience.forex.utilities.oanda_tools import get_oanda_headers
from badassdatascience.forex.utilities.oanda_tools import price_type_map

class CandlePull():

    #
    # Constructor
    #
    def __init__(
        self,
        config_file,
        count,
        granularity,
        instrument,
        price_types,
        error_retry_interval = 5,
        keep_complete_only = True
    ):

        #
        # command line arguments
        #
        self.config_file = config_file
        self.count = count
        self.granularity = granularity
        self.instrument = instrument
        self.price_types = price_types
        self.error_retry_interval = error_retry_interval
        self.keep_complete_only = keep_complete_only

        #
        # initialize (hard-coded)
        #
        self.timezone_to_use = 'America/Toronto'   # Don't change this!
        self.start_time = int(datetime.datetime(2010, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc).timestamp())
        
        #
        # additional initialization
        #
        self.timezone = pytz.timezone(self.timezone_to_use)  # not sure we are using this... check later
        self.price_type_list = [price_type_map[q] for q in self.price_types]
        self.end_date_original = int(time.mktime(datetime.datetime.now().timetuple()))

    #
    # Get the header information needed for Oanda requests
    #
    def get_headers(self):
        with open(self.config_file) as f:
            self.config = json.load(f)
        self.headers = get_oanda_headers(self.config)

    #
    # request forex price/volume candlesticks from Oanda
    #
    def get_instrument_candlesticks(self, end_date):
        url = (
            self.config['server']
            + '/v3/instruments/' + self.instrument
            + '/candles?count=' + str(self.count)
            + '&price=' + self.price_types
            + '&granularity=' + self.granularity
            + '&to=' + str(end_date)
        )
        
        worked = False
        while not worked:
            try:
                r = requests.get(url, headers = self.headers)
                worked = True
            except:
                time.sleep(error_retry_interval)
        
        rj = r.json()
        return rj

    #
    # compute additional forex candlestick features
    #
    def compute_candle_features(self):
        
        finished = False
        end_date = self.end_date_original

        self.insert_many_list = []

        # loop through the timestamp ranges for each set of n=count values
        while not finished:

            # retrieve the instrument candlesticks from the Oanda server
            rj = self.get_instrument_candlesticks(end_date) # instrument, count, price_types, granularity, end_date)        
            candlesticks = rj['candles']

            #
            # deal with timestamps and time-related content
            #
            date_list = []
            for candle in candlesticks:

                candle['instrument'] = self.instrument.replace('_', '/')
                candle['granularity'] = self.granularity
                candle['time'] = int(float(candle['time']))
                time_dt = datetime.datetime.fromtimestamp(candle['time'], tz = self.timezone)
                candle['time_iso'] = time_dt.isoformat()
                candle['weekday'] = time_dt.weekday()
                candle['hour'] = time_dt.hour

                for price_type in self.price_type_list:
                    for candlestick_component in candle[price_type].keys():
                        candle[price_type + '_' + candlestick_component] = float(candle[price_type][candlestick_component])
                    candle[price_type + '_return'] = candle[price_type + '_c'] - candle[price_type + '_o']
                    candle[price_type + '_volatility'] = candle[price_type + '_h'] - candle[price_type + '_l']
            
                for price_type in self.price_type_list:
                    del(candle[price_type])

                
                if self.keep_complete_only:
                    if candle['complete']:    
                        self.insert_many_list.append(candle)
                else:
                    self.insert_many_list.append(candle)

                date_list.append(candle['time'])


            # Are we done?
            if (len(date_list) < count) or (min(date_list) < self.start_time):
                finished = True
            else:
                # prepare for the next iteration
                end_date = min(date_list) - 0.1

    #
    # Create a dataframe
    #
    def create_dataframe(self):
        self.df = pd.DataFrame(self.insert_many_list).sort_values(by = ['instrument', 'time'])
        self.df = self.df[self.df['time'] >= int(self.start_time)]
        self.df = self.df.reset_index().copy()

    #
    # QA
    #
    def qa(self):
        print(len(self.df.index) == len(self.df[['time']].drop_duplicates()))
        print(len(t.df.index) == len(t.df['time'].unique()))

    #
    # Compute everything
    #
    def fit(self):
        self.get_headers()
        self.compute_candle_features()
        self.create_dataframe()
        self.qa()

    #
    # plot
    #
    def plot(self, savepath = None):
        plt.figure()
        plt.plot(self.df['time'], self.df['mid_c'])

        if savepath == None:
            plt.show()
        else:
            plt.savefig(savepath)

        plt.close()
