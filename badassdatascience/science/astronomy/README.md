# Astronomy Analyses

This directory contains code and documentation for astronomy-related posts on the Badass Data Science blog:

## Solar Noon

Needed to know when solar noon occurs on a given day for a photography project. Calculated it using [SolarNoon-demo.ipynb](SolarNoon-demo.ipynb), which in turn calls the module [SolarNoon.py](SolarNoon.py) to perform the heavy lifting.

This code retrieves ephemeris data from the [PyEphem](https://rhodesmill.org/pyephem) module.

##### Example:  Solar Noon Calculation

This looks nicer in the [SolarNoon-demo.ipynb](SolarNoon-demo.ipynb) notebook...
```
from SolarNoon import SolarNoon
import datetime

sn = SolarNoon(
    latitude = 32.7157,
    longitude = -117.1611,
    local_timezone = 'America/Los_Angeles',
    date_to_use_local = datetime.date(2019, 9, 9),
)

sn.fit()

sn.summary()
```
Result:
```
{'Date': '2019-09-09',
 'Latitude': 32.7157,
 'Longitude': -117.1611,
 'Solar Noon': '12:46:17',
 'Timezone': 'America/Los_Angeles'}
```