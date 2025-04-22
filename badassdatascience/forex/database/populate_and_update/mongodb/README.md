```
/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forex/data/DEVELOPMENT.json

output_temp

python3 get_candles.py --config-file /home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forex/data/DEVELOPMENT.json --count 5000 --granularity M1 --output-file output/M1.json --now --instruments "EUR_USD,USD_CAD,USD_JPY,USD_CHF,AUD_USD,GBP_USD,NZD_USD" --price-types BAM







use forex;

db.createCollection("candlesticks");

db.candlesticks.createIndex({instrument : 1, time : 1, complete : 1}, {unique : true});




papermill oop_get_candles_notebook.ipynb output/candles_EUR_USD_M15.ipynb -p granularity M15 -p instrument EUR_USD
papermill oop_get_candles_notebook.ipynb output/candles_USD_CAD_M15.ipynb -p granularity M15 -p instrument USD_CAD
papermill oop_get_candles_notebook.ipynb output/candles_USD_JPY_M15.ipynb -p granularity M15 -p instrument USD_JPY
papermill oop_get_candles_notebook.ipynb output/candles_USD_CHF_M15.ipynb -p granularity M15 -p instrument USD_CHF
papermill oop_get_candles_notebook.ipynb output/candles_AUD_USD_M15.ipynb -p granularity M15 -p instrument AUD_USD
papermill oop_get_candles_notebook.ipynb output/candles_GBP_USD_M15.ipynb -p granularity M15 -p instrument GBP_USD
papermill oop_get_candles_notebook.ipynb output/candles_NZD_USD_M15.ipynb -p granularity M15 -p instrument NZD_USD


source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_EUR_USD_M15.ipynb -p granularity M15 -p instrument EUR_USD

source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_CAD_M15.ipynb -p granularity M15 -p instrument USD_CAD

source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_JPY_M15.ipynb -p granularity M15 -p instrument USD_JPY

source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_USD_CHF_M15.ipynb -p granularity M15 -p instrument USD_CHF

source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_AUD_USD_M15.ipynb -p granularity M15 -p instrument AUD_USD

source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_GBP_USD_M15.ipynb -p granularity M15 -p instrument GBP_USD

source /home/emily/activate_stuff.sh; export BDS_HOME=/home/emily/Desktop/projects/test/badass-data-science; export PYTHONPATH=$PYTHONPATH:$BDS_HOME; /home/emily/venvs/ml/bin/papermill $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/oop_get_candles_notebook.ipynb $BDS_HOME/badassdatascience/forex/database/populate_and_update/mongodb/output/candles_NZD_USD_M15.ipynb -p granularity M15 -p instrument NZD_USD


```