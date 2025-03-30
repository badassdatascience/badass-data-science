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


57f47108-380d-4e78-aeb9-7ebccd07ec65----6f467d3d-510d-44c5-80ab-9c23c9b6801a
57f47108-380d-4e78-aeb9-7ebccd07ec65----798bb099-0e9a-4a21-88c9-a978d48215ea # better