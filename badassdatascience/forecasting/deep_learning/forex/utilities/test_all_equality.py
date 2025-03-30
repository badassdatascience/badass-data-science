
#from itertools import groupby
#
#import pyspark.sql.functions as f
#from pyspark.sql.types import BooleanType
#
#def test_all_equality(*a_list):
#    g = groupby(a_list)
#    return next(g, True) and not next(g, False)
#
#udf_test_all_equality = f.udf(test_all_equality, BooleanType())
