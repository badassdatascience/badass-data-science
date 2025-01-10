
def convert_datetime_hms_to_hour_decimal(datetime_hhhh_mm_ss):
    hour = datetime_hhhh_mm_ss.hour
    minute = datetime_hhhh_mm_ss.minute
    second = datetime_hhhh_mm_ss.second
    new_minute = minute + (second / 60.)
    new_hour = hour + (new_minute / 60.)
    return new_hour
