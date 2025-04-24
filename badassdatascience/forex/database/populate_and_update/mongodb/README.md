# Populate and Update a Mongo Database Containing Currency-Pair Features

The module "oop_get_candles.py" and the accompanying notebook "oop_get_candles_notebook.ipynb" retrieve currency-pair prices for a given set of parameters (e.g., currency-pair name or time series granularity) and deposits it into a given MongoDB database.

The notebook is designed to run automatically with Papermill (see https://papermill.readthedocs.io/en/latest), which automatically handles parameterization when updating the database automatically on a schedule. See Appendix One below for examples of Papermill commands designed for this purpose. See Appendix Two below for Airflow or Cron commands that implement a given schedule.

Each time Papermill is run by either Airflow or Cron, completed notebooks run according to the given parameters are created in the directory $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output.

## Preparing the Mongo Database
```
cd $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb

mkdir output

use forex;

db.createCollection("candlesticks");

db.candlesticks.createIndex({instrument : 1, granularity : 1, time : 1, complete : 1}, {unique : true});
```

## Query Example:  Recovering Candlestick Features
```
mongosh

test> use forex;

forex> db.candlesticks.find({instrument: 'EUR/USD', granularity: 'H1', time : {$gt : 1579111200}, complete: true}).limit(2)
```

#### Example Query Result
```
[
  {
    _id: ObjectId('68097c78da2b3e213f23ea62'),
    complete: true,
    volume: 1155,
    time: 1579114800,
    instrument: 'EUR/USD',
    granularity: 'H1',
    time_iso: '2020-01-15T14:00:00-05:00',
    weekday: 2,
    hour: 14,
    bid_o: 1.1151,
    bid_h: 1.11535,
    bid_l: 1.11489,
    bid_c: 1.11499,
    bid_return: -0.0001100000000000545,
    bid_volatility: 0.000460000000000127,
    ask_o: 1.11525,
    ask_h: 1.11547,
    ask_l: 1.11501,
    ask_c: 1.1151,
    ask_return: -0.0001500000000000945,
    ask_volatility: 0.00045999999999990493,
    mid_o: 1.11518,
    mid_h: 1.11541,
    mid_l: 1.11495,
    mid_c: 1.11504,
    mid_return: -0.000140000000000029,
    mid_volatility: 0.00045999999999990493
  },
  {
    _id: ObjectId('68097c78da2b3e213f23ea63'),
    complete: true,
    volume: 1030,
    time: 1579118400,
    instrument: 'EUR/USD',
    granularity: 'H1',
    time_iso: '2020-01-15T15:00:00-05:00',
    weekday: 2,
    hour: 15,
    bid_o: 1.115,
    bid_h: 1.11524,
    bid_l: 1.11475,
    bid_c: 1.1152,
    bid_return: 0.00019999999999997797,
    bid_volatility: 0.0004900000000001015,
    ask_o: 1.11511,
    ask_h: 1.11537,
    ask_l: 1.11488,
    ask_c: 1.11531,
    ask_return: 0.00019999999999997797,
    ask_volatility: 0.0004899999999998794,
    mid_o: 1.11506,
    mid_h: 1.1153,
    mid_l: 1.11482,
    mid_c: 1.11526,
    mid_return: 0.00019999999999997797,
    mid_volatility: 0.00048000000000003595
  }
]
```

## Appendix One:  Papermill Commands

```
papermill oop_get_candles_notebook.ipynb output/candles_EUR_USD_M15.ipynb -p granularity M15 -p instrument EUR_USD
papermill oop_get_candles_notebook.ipynb output/candles_USD_CAD_M15.ipynb -p granularity M15 -p instrument USD_CAD
papermill oop_get_candles_notebook.ipynb output/candles_USD_JPY_M15.ipynb -p granularity M15 -p instrument USD_JPY
papermill oop_get_candles_notebook.ipynb output/candles_USD_CHF_M15.ipynb -p granularity M15 -p instrument USD_CHF
papermill oop_get_candles_notebook.ipynb output/candles_AUD_USD_M15.ipynb -p granularity M15 -p instrument AUD_USD
papermill oop_get_candles_notebook.ipynb output/candles_GBP_USD_M15.ipynb -p granularity M15 -p instrument GBP_USD
papermill oop_get_candles_notebook.ipynb output/candles_NZD_USD_M15.ipynb -p granularity M15 -p instrument NZD_USD
```
## Appendix Two:  Cron or Airflow Commands

#### M15 Granularity (Every 15 Minutes)
```
*/15 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_EUR_USD_M15.ipynb -p granularity M15 -p instrument EUR_USD

*/15 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_CAD_M15.ipynb -p granularity M15 -p instrument USD_CAD

*/15 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_JPY_M15.ipynb -p granularity M15 -p instrument USD_JPY

*/15 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_CHF_M15.ipynb -p granularity M15 -p instrument USD_CHF

*/15 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_AUD_USD_M15.ipynb -p granularity M15 -p instrument AUD_USD

*/15 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_GBP_USD_M15.ipynb -p granularity M15 -p instrument GBP_USD

*/15 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_NZD_USD_M15.ipynb -p granularity M15 -p instrument NZD_USD
```

#### H1 Granularity (Every Hour)
```
5 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_EUR_USD_H1.ipynb -p granularity H1 -p instrument EUR_USD

5 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_CAD_H1.ipynb -p granularity H1 -p instrument USD_CAD

5 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_JPY_H1.ipynb -p granularity H1 -p instrument USD_JPY

5 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_CHF_H1.ipynb -p granularity H1 -p instrument USD_CHF

5 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_AUD_USD_H1.ipynb -p granularity H1 -p instrument AUD_USD

5 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_GBP_USD_H1.ipynb -p granularity H1 -p instrument GBP_USD

5 * * * * source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_NZD_USD_H1.ipynb -p granularity H1 -p instrument NZD_USD
```
