import re

#
# Split a string by its digits
#
# From Google search "python string split by digit" (AI summary result):
#
def split_string_by_digit(text):
    """Splits a string into a list of substrings, separated by digits."""
    return re.split(r'(\d+)', text)
