# Church to Bar Ratio per U.S. County (4th Edition)

The brighter the color, the higher the church to bar ratio. Counties missing data necessary for the computation are shown in black.
Method

From the latest (2022) County Business Patterns data published by the U.S. Census Bureau at [https://data.census.gov/table/CBP2022.CB2200CBP](https://data.census.gov/table/CBP2022.CB2200CBP), I extracted the number of establishments in each county that have NAICS codes 813110 (places of worship including churches, temples, mosques, synagogues, etc.) and 722410 (bars, taverns, drink-serving nightclubs, etc.). For each county, I then divided the number of NAICS 813110 establishments by the number of NAICS 722410 establishments to get the “church to bar ratio”. Before dividing I added one (a pseudocount) to each value to prevent division by zero errors. Finally, I partitioned the resulting ratio distribution into nine color groups, and plotted each county’s ratio color on the map. Used log-transformed values when creating the partitions. Counties missing both NAICS codes in the source data are shown in black on the map. Source code implementing these calculations is attached, to facilitate peer review.

I typically publish these results every two years as new data becomes available. This is the fourth such release.
Results

The "Bible Belt" shows up in brighter colors, as we'd expect. In Texas, Travis County--the home of Austin--is a booze cesspool compared to neighboring (and more straight-edge) Williamson County, again, as we'd expect. These two observations validate the method.
Possible bias in the result

If NAICS code 813110 excludes church facilities with unpaid staff and clergy—i.e., if the Census Bureau does not to consider them “places of industry” since no one is getting paid to work there—the LDS presence in the west will be severely underrepresented. This warrants further review of NAICS measurement methodology before producing future map updates. Reader comments on the appropriateness of NAICS industry codes for this study are very welcome!
