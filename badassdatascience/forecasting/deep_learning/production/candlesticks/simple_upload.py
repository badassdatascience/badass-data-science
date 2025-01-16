
import glob
import sys
import json

output_directory = 'output'
final_file = 'booger_H1.json'


done_list = sorted([int(x.split('/')[-1].split('_')[-1].split('.')[0]) for x in glob.glob(output_directory + '/loop/*.json') if x.find('meta') == -1])

#
# assemble
#
assembled_candles = {}
for i in done_list:
    with open(output_directory + '/loop/candles_' + str(i) + '.json') as f:
        local_candles = json.load(f)

    for instrument in local_candles.keys():
        if not instrument in assembled_candles:
            assembled_candles[instrument] = []
        assembled_candles[instrument].extend(local_candles[instrument])

#
# verify
#
for instrument in assembled_candles.keys():
    time_list = []
    for item in assembled_candles[instrument]:
        time_list.append(int(float(item['time'])))

    print(time_list == sorted(time_list))
    print(len(time_list))

#
# save
#
with open(output_directory + '/' + final_file, 'w') as f:
    json.dump(assembled_candles, f, indent=2)
