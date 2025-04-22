```
/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forex/data/DEVELOPMENT.json

output_temp

python3 get_candles.py --config-file /home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forex/data/DEVELOPMENT.json --count 5000 --granularity M1 --output-file output/M1.json --now --instruments "EUR_USD,USD_CAD,USD_JPY,USD_CHF,AUD_USD,GBP_USD,NZD_USD" --price-types BAM







use forex;

db.createCollection("candlesticks");

db.candlesticks.createIndex({instrument : 1, time : 1, complete : 1}, {unique : true});




papermill oop_get_candles_notebook.ipynb output/candles_EUR_USD_M15.ipynb -p granularity M15 -p instrument EUR_USD


```