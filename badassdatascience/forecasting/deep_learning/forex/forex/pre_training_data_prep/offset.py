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
