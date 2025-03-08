import pandas as pd

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

    output_file_name_and_path = config['directory_output'] + '/' + config['filename_offset']
    
    pdf_shifted_weekday.to_parquet(output_file_name_and_path)

    
def merge_offset_map(**config):

    candlesticks_file_name_and_path = config['directory_output'] + '/' + config['filename_timezone_added']    
    weekday_shifted_file_name_and_path = config['directory_output'] + '/' + config['filename_offset']

    pdf_candlesticks = pd.read_parquet(candlesticks_file_name_and_path)
    pdf_shifted = pd.read_parquet(weekday_shifted_file_name_and_path)

    pdf = (
        pd.merge(
            pdf_candlesticks,
            pdf_shifted,
            on = ['weekday_tz', 'hour_tz'],
            how = 'left',
        )
        .sort_values(by = ['datetime_tz'])
    )
    
    candlesticks_weekday_merged_file_name_and_path = config['directory_output'] + '/' + config['filename_weekday_shift_merged']
    pdf.to_parquet(candlesticks_weekday_merged_file_name_and_path)
    
