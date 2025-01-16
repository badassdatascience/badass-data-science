```
cd $APP_HOME/badassdatascience/forecasting/deep_learning/stuff/production/initialize_database

python initialize_timeseries.py

cd $APP_HOME/badassdatascience/forecasting/deep_learning/stuff/production/candlesticks

mkdir output

mkdir output/loop

python get_candles_loop.py --config-file ../../data/DEVELOPMENT.json --count 5000 --granularity M1 --output-directory output --now --price-types BAM --instruments EUR_USD

```
