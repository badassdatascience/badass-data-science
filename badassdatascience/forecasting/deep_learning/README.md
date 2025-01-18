```
export BDS_HOME=$HOME/Desktop/projects/test/badass-data-science

cd $BDS_HOME

cd $BDS_HOME/badassdatascience/forecasting/deep_learning/production/initialize_database

python initialize_timeseries.py

cd $BDS_HOME/badassdatascience/forecasting/deep_learning/production/candlesticks

mkdir output

mkdir output/loop

python get_candles_loop.py --config-file ../../data/DEVELOPMENT.json --count 5000 --granularity M1 --output-directory output --now --price-types BAM --instruments EUR_USD

python3 /home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/production/initialize_database/load_candles_bulk.py --source-directory /home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/production/candlesticks/output/loop --interval-name Minute



```