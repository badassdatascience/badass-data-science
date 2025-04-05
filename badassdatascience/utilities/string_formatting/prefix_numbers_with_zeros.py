

#
# Given a number, particulary an integer, this prefixes a string
# representation of that number with values such that the desired
# (base 10) order of magnitude is returned.
#
# For example:  add_prefix_zeros(34, 5) returns "00034".
#
# This is most useful for ensuring proper sorting of filenames
# containing integers in their names.
#
def add_prefix_zeros(number, base_ten_order_of_magnitude):
    number_str = str(number)
    left_of_decimal = number_str.split('.')[0]
    zeros_to_pad_with = base_ten_order_of_magnitude - len(left_of_decimal)
    return ''.join(['0'] * zeros_to_pad_with) + number_str

def test():
    assert add_prefix_zeros(34, 5) == '00034'
    assert add_prefix_zeros(34, 6) == '000034'
    assert add_prefix_zeros(34, 2) == '34'
    assert add_prefix_zeros(34, 10) == '0000000034'
    
if __name__ == "__main__":
    test()

