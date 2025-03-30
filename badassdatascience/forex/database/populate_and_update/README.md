```
python3 $BDS_HOME/badassdatascience/forex/database/populate_and_update/get_candles_loop.py --config-file $BDS_HOME/badassdatascience/forex/data/DEVELOPMENT.json --count 5000 --granularity D --output-directory output --now --price-types BAM --instruments EUR_USD

cd $BDS_HOME

python3 badassdatascience/forex/database/populate_and_update/load_candles_bulk.py --source-directory badassdatascience/forex/output/loop --interval-name Day
```