# This code assumes the broker stops trading at 17:00 ET on Friday
# and resumes at 17:00 ET Sunday. Need to verify this assumption, but
# I think it is correct given my previous effort

import pandas as pd
import datetime

def generate_offset_map(**config):

    hour_list = []
    weekday_list = []
    shifted_list = []

    shifted_list.extend([0] * 17)

    for i in range(0, 4):
        weekday_list.extend([i] * 24)
        hour_list.extend(sorted(list(range(0, 24))))

        if i >= 1:
            shifted_list.extend([i] * 24)

    shifted_list.extend([4] * 24)

    weekday_list.extend([4] * 17)
    hour_list.extend(sorted(list(range(0, 17))))

    weekday_list.extend([6] * 7)
    hour_list.extend(sorted(list(range(17, 24))))
    shifted_list.extend([0] * 7)

    pdf_shifted_weekday = pd.DataFrame({'weekday_tz' : weekday_list, 'hour_tz' : hour_list, 'weekday_shifted' : shifted_list})

    output_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_offset']
    
    pdf_shifted_weekday.to_parquet(output_file_name_and_path)


# need to investigate why some of the resulting rows have null values:
def merge_offset_map(**config):

    candlesticks_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_timezone_added']    
    weekday_shifted_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_offset']

    pdf_candlesticks = pd.read_parquet(candlesticks_file_name_and_path)
    pdf_shifted = pd.read_parquet(weekday_shifted_file_name_and_path)

    pdf = (
        pd.merge(
            pdf_candlesticks,
            pdf_shifted,
            on = ['weekday_tz', 'hour_tz'],
            how = 'left',
        )
        .dropna()
        .sort_values(by = ['datetime_tz'])
        .reset_index()
        .drop(columns = ['index'])
    )
    
    pdf['weekday_shifted'] = pdf['weekday_shifted'].astype('uint8')
    
    candlesticks_weekday_merged_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_weekday_shift_merged']
    pdf.to_parquet(candlesticks_weekday_merged_file_name_and_path)


#
# perform the date shift
#
def shift_days_and_hours_as_needed(**config):

    candlesticks_weekday_merged_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_weekday_shift_merged']
    df = pd.read_parquet(candlesticks_weekday_merged_file_name_and_path)

    df['original_date'] = [x.date() for x in df['datetime_tz']]
    df['to_shift'] = df['weekday_shifted'] - df['weekday_tz']

    pdf_date_to_shift = (
        df
        .sort_values(by = 'datetime_tz')
        [['weekday_tz', 'hour_tz', 'weekday_shifted', 'original_date', 'to_shift']]
        .drop_duplicates()  # Should be no duplicates
    )

    new_date_list = []
    for i, row in pdf_date_to_shift.iterrows():
        if row['to_shift'] > 0:
            delta = datetime.timedelta(days = row['to_shift'])
            new_date_list.append(row['original_date'] + delta)
        elif row['to_shift'] == -6:
            delta = datetime.timedelta(days = 1)
            new_date_list.append(row['original_date'] + delta)
        else:
            new_date_list.append(row['original_date'])

    pdf_date_to_shift['original_date_shifted'] = new_date_list

    pdf = (
        pd.merge(
            df.drop(columns = ['to_shift']),
            pdf_date_to_shift,
            on = ['weekday_tz', 'hour_tz', 'weekday_shifted', 'original_date'],
            how = 'left',
        )
        .drop(columns = ['original_date', 'to_shift'])
        .sort_values(by = ['timestamp'])
    )

    # Should this be here or somewhere else?
    pdf = pdf[~pdf['weekday_shifted'].isna()]

    shifted_as_needed_file_name_and_path = config['directory_output'] + '/' + config['dag_run'].run_id + '/' + config['filename_shift_days_and_hours_as_needed']
    pdf.to_parquet(shifted_as_needed_file_name_and_path)
