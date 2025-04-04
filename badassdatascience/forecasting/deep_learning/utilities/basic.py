import numpy as np
import pyspark.sql.functions as f
from pyspark.sql.types import ArrayType, IntegerType, FloatType

#
# not sure about best practices here...
#
def difference_an_array(the_array, seconds_divisor):
    return [int((y - x) / seconds_divisor) for x, y in zip(the_array[0:-1], the_array[1:])]

udf_difference_an_array = f.udf(difference_an_array, ArrayType(IntegerType()))

#25/01/31 16:47:06 ERROR Executor: Exception in task 7.0 in stage 75.0 (TID 10000)
#org.apache.spark.api.python.PythonException: Traceback (most recent call last):
#  File "/home/emily/Desktop/projects/test/badass-data-science/badassdatascience/forecasting/deep_learning/utilities/basic.py", line 28, in deal_with_offset
#    items[position_column] = values_list[i]
#IndexError: index 1455 is out of bounds for axis 0 with size 1444

def deal_with_offset(values_list, diff_timestamp_list, max_array_length):

    items = np.empty([max_array_length])
    items[:] = np.nan
    position_column = -1

    for i, diff in enumerate(diff_timestamp_list):
        position_column += diff
        items[position_column] = values_list[i]
            
    result = [float(x) for x in items]
    return result

udf_deal_with_offset = f.udf(deal_with_offset, ArrayType(FloatType()))


# https://stackoverflow.com/questions/6518811/interpolate-nan-values-in-a-numpy-array
def nan_helper(y):
    return np.isnan(y), lambda z: z.nonzero()[0]


#
# https://stackoverflow.com/questions/70899029/how-to-get-all-rows-with-null-value-in-any-column-in-pyspark
#
def nan_count_spark(df, column_name):
    result = df.select(
        f.count(f.when(f.isnan(column_name)==True, f.col(column_name))).alias(column_name + '_NaN_count')
    )
    return result

