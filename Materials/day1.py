# Import SparkSession
from pyspark.sql import SparkSession
# PySpark, col is a function from the pyspark.sql.functions module that is used to refer to a column in a DataFrame.
from pyspark.sql.functions import col
# Avg function is used to calculate the average (mean) value of a column in a DataFrame
from pyspark.sql.functions import avg

# Create SparkSession 
spark = SparkSession.builder.master("local[1]").appName("test1").getOrCreate() 


data = [('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)]

columns = ["firstname","middlename","lastname","dob","gender","salary"]

# Create DataFrame
df = spark.createDataFrame(data=data, schema = columns)

#show:- Display the first few rows of a DataFrame.
df.show()

#(or)

# Create DataFrame
df1 = spark.createDataFrame([('James','','Smith','1991-04-01','M',3000),
  ('Michael','Rose','','2000-05-19','M',4000),
  ('Robert','','Williams','1978-09-05','M',4000),
  ('Maria','Anne','Jones','1967-12-01','F',4000),
  ('Jen','Mary','Brown','1980-02-17','F',-1)
], ["firstname","middlename","lastname","dob","gender","salary"])

df1.show()

#print the dll of data frame  details
df.printSchema()

#Creating Temp view
df.createOrReplaceTempView("employee")

# Execute a SQL query to select all data from the 'employee' view
result_df = spark.sql("SELECT * FROM employee")

# Show the first few rows of the result
result_df.show()

# Execute a SQL query to select all data from the 'employee' view and show the result
spark.sql("SELECT * FROM employee").show()

# Retrieve the data from the temporary view
result_df = spark.table("employee")

# Show the first few rows of the result
result_df.show()

#Keywords and usage with example

df = spark.createDataFrame([(1, 'Alice'), (2, 'Bob'),(1, 'Alice'), (2, 'Bob'),(5,'Davic')], ['id', 'name'])

#select:- Select specific columns from a DataFrame
df.select('name').show()

#filter:- Filter rows based on a condition
df.filter(col('id') > 1).show()

#groupBy:- Group rows by one or more columns.
df.groupBy('name').count().show()

#agg:- Aggregate data using functions like sum, avg, etc.
df.groupBy('name').agg(avg('id').alias('average_id')).show()

df2 = spark.createDataFrame([(1, 'HR'), (2, 'Engineering'),(5, 'Doctor')], ['id', 'department'])

#join:-Join two DataFrames based on a key column.
df.join(df2, on='id').show()

#withColumn:- Add or replace a column in a DataFrame.
from pyspark.sql.functions import lit

df.withColumn('new_column', lit(10)).show()

#drop:- Drop a column from a DataFrame.
df.drop('name').show()

#orderBy:- Order rows based on one or more columns.
df.orderBy('id', ascending=False).show()

#distinct:- Remove duplicate rows.
df.distinct().show()

#collect:- Collect all rows of the DataFrame as a list of Row objects.
rows = df.collect()
for row in rows:
    print(row)

#describe:- Provide summary statistics of DataFrame columns.
df.describe().show()

# cache / persist:- Cache or persist the DataFrame in memory for faster access.
df.cache().count()  # Trigger cache by performing an action


#Alias:- Rename a column or DataFrame.
df.select(col('name').alias('employee_name')).show()

#withColumnRenamed:- Rename a column in a DataFrame.
df.withColumnRenamed('name', 'employee_name').show()

#dropna:- Remove rows with null or NaN values.
df.dropna().show()

#fillna:- Replace null or NaN values with a specified value.
df.fillna({'id': 0, 'name': 'Unknown'}).show()

#na.drop / na.fill:- Drop or fill missing values using the na functions.
# Drop rows with any null values
df.na.drop().show()
# Fill null values with a specific value
df.na.fill({'name': 'Unknown'}).show()

#cast:- Change the data type of a column.
df.withColumn('id', col('id').cast('string')).show()

#explode:- Convert an array or map column to multiple rows.

from pyspark.sql.functions import explode, array
df = spark.createDataFrame([(1, [2, 3]), (4, [5, 6])], ['id', 'values'])
df.select('id', explode('values')).show()

#concat:- Concatenate multiple columns into a single column.

from pyspark.sql.functions import concat, col
df.withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name'))).show()

#split:- Split a column into multiple columns based on a delimiter.

from pyspark.sql.functions import split
df = spark.createDataFrame([('John Doe',)], ['name'])
df.select(split(col('name'), ' ').alias('split_name')).show()

#date_format:- Format a date column as a string.

from pyspark.sql.functions import date_format
df.withColumn('formatted_date', date_format(col('date_column'), 'yyyy-MM-dd')).show()

#substring:- Extract a substring from a column.

df.withColumn('sub_name', col('name').substr(1, 3)).show()

#when / otherwise:- Perform conditional operations on columns.

from pyspark.sql.functions import when
df.withColumn('category', when(col('id') > 1, 'High').otherwise('Low')).show()

27. lag / lead
Purpose: Access data from a previous or next row.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import lag

windowSpec = Window.orderBy('id')
df.withColumn('prev_id', lag('id', 1).over(windowSpec)).show()
28. rank / dense_rank
Purpose: Assign rank to rows based on ordering.
Example:
python
Copy code
from pyspark.sql.functions import rank
from pyspark.sql.window import Window

windowSpec = Window.orderBy('id')
df.withColumn('rank', rank().over(windowSpec)).show()
29. crosstab
Purpose: Compute the cross-tabulation of two columns.
Example:
python
Copy code
df.crosstab('column1', 'column2').show()
30. sample
Purpose: Sample rows from a DataFrame.
Example:
python
Copy code
df.sample(fraction=0.5).show()  # Sample 50% of the rows
31. repartition
Purpose: Change the number of partitions in a DataFrame.
Example:
python
Copy code
df.repartition(10).show()
32. coalesce
Purpose: Reduce the number of partitions in a DataFrame.
Example:
python
Copy code
df.coalesce(1).write.csv('output.csv')
33. foreach
Purpose: Apply a function to each row in the DataFrame.
Example:
python
Copy code
def print_row(row):
    print(row)

df.foreach(print_row)
34. printSchema
Purpose: Print the schema of the DataFrame.
Example:
python
Copy code
df.printSchema()
35. union
Purpose: Combine rows from two DataFrames with the same schema.
Example:
python
Copy code
df1 = spark.createDataFrame([(1, 'Alice')], ['id', 'name'])
df2 = spark.createDataFrame([(2, 'Bob')], ['id', 'name'])
df1.union(df2).show()
36. subtract
Purpose: Subtract rows from one DataFrame that are present in another DataFrame.
Example:
python
Copy code
df1.subtract(df2).show()
37. intersect
Purpose: Find rows that are common in both DataFrames.
Example:
python
Copy code
df1.intersect(df2).show()

38. add_months
Purpose: Add a specified number of months to a date column.
Example:
python
Copy code
from pyspark.sql.functions import add_months
df.withColumn('future_date', add_months(col('date_column'), 3)).show()
39. current_date
Purpose: Get the current date.
Example:
python
Copy code
from pyspark.sql.functions import current_date
df.withColumn('today', current_date()).show()
40. current_timestamp
Purpose: Get the current timestamp.
Example:
python
Copy code
from pyspark.sql.functions import current_timestamp
df.withColumn('now', current_timestamp()).show()
41. datediff
Purpose: Calculate the difference in days between two dates.
Example:
python
Copy code
from pyspark.sql.functions import datediff
df.withColumn('days_diff', datediff(col('end_date'), col('start_date'))).show()
42. months_between
Purpose: Calculate the number of months between two dates.
Example:
python
Copy code
from pyspark.sql.functions import months_between
df.withColumn('months_diff', months_between(col('end_date'), col('start_date'))).show()
43. year / month / dayofmonth
Purpose: Extract year, month, or day of the month from a date column.
Example:
python
Copy code
from pyspark.sql.functions import year, month, dayofmonth
df.withColumn('year', year(col('date_column'))).show()
df.withColumn('month', month(col('date_column'))).show()
df.withColumn('day', dayofmonth(col('date_column'))).show()
44. expr
Purpose: Execute a SQL expression as a DataFrame operation.
Example:
python
Copy code
from pyspark.sql.functions import expr
df.withColumn('new_column', expr('id * 10')).show()
45. array
Purpose: Create an array column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import array
df.withColumn('array_col', array(col('col1'), col('col2'))).show()
46. map
Purpose: Create a map column from multiple key-value pairs.
Example:
python
Copy code
from pyspark.sql.functions import create_map
df.withColumn('map_col', create_map(col('key1'), col('value1'), col('key2'), col('value2'))).show()
47. isNull / isNotNull
Purpose: Check if a column value is null or not null.
Example:
python
Copy code
df.filter(col('name').isNull()).show()
df.filter(col('name').isNotNull()).show()
48. lag / lead
Purpose: Access data from a previous or next row within a window specification.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead

windowSpec = Window.orderBy('id')
df.withColumn('prev_id', lag('id', 1).over(windowSpec)) \
  .withColumn('next_id', lead('id', 1).over(windowSpec)).show()
49. cume_dist
Purpose: Calculate the cumulative distribution of a column.
Example:
python
Copy code
from pyspark.sql.functions import cume_dist
windowSpec = Window.orderBy('id')
df.withColumn('cume_dist', cume_dist().over(windowSpec)).show()
50. ntile
Purpose: Divide the data into a specified number of buckets.
Example:
python
Copy code
from pyspark.sql.functions import ntile
windowSpec = Window.orderBy('id')
df.withColumn('quartile', ntile(4).over(windowSpec)).show()
51. approxQuantile
Purpose: Compute approximate quantiles of a numeric column.
Example:
python
Copy code
quantiles = df.approxQuantile('id', [0.25, 0.5, 0.75], 0.05)
print(quantiles)
52. describe
Purpose: Generate descriptive statistics for numeric columns.
Example:
python
Copy code
df.describe().show()
53. repartitionByRange
Purpose: Repartition DataFrame based on column ranges.
Example:
python
Copy code
df.repartitionByRange(4, 'id').show()
54. coalesce
Purpose: Reduce the number of partitions in a DataFrame.
Example:
python
Copy code
df.coalesce(2).write.format('csv').save('output.csv')
55. cache / persist
Purpose: Cache or persist the DataFrame to memory or disk.
Example:
python
Copy code
df.cache().count()  # Triggers the caching
56. unpersist
Purpose: Remove the DataFrame from the cache.
Example:
python
Copy code
df.unpersist()
57. sql
Purpose: Execute SQL queries against the DataFrame.
Example:
python
Copy code
df.createOrReplaceTempView('temp_view')
result = spark.sql('SELECT * FROM temp_view WHERE id > 1')
result.show()
58. toPandas
Purpose: Convert a PySpark DataFrame to a Pandas DataFrame.
Example:
python
Copy code
pandas_df = df.toPandas()
print(pandas_df.head())
59. toDF
Purpose: Convert an RDD or list to a DataFrame with specified column names.
Example:
python
Copy code
rdd = spark.sparkContext.parallelize([(1, 'Alice'), (2, 'Bob')])
df = rdd.toDF(['id', 'name'])
df.show()
60. printSchema
Purpose: Print the schema of the DataFrame.
Example:
python
Copy code
df.printSchema()

61. concat_ws
Purpose: Concatenate multiple columns into a single column with a specified separator.
Example:
python
Copy code
from pyspark.sql.functions import concat_ws
df.withColumn('full_name', concat_ws(' ', col('first_name'), col('last_name'))).show()
62. array_contains
Purpose: Check if an array column contains a specific value.
Example:
python
Copy code
from pyspark.sql.functions import array_contains
df.filter(array_contains(col('array_column'), 'value')).show()
63. element_at
Purpose: Access an element at a specific position in an array or map.
Example:
python
Copy code
from pyspark.sql.functions import element_at
df.withColumn('second_element', element_at(col('array_column'), 2)).show()
64. first / last
Purpose: Get the first or last value in a group.
Example:
python
Copy code
from pyspark.sql.functions import first, last
df.groupBy('group_column').agg(first('value').alias('first_value')).show()
df.groupBy('group_column').agg(last('value').alias('last_value')).show()
65. size
Purpose: Get the size of an array or map.
Example:
python
Copy code
from pyspark.sql.functions import size
df.withColumn('array_size', size(col('array_column'))).show()
66. regexp_replace
Purpose: Replace substrings matching a regular expression with a specified string.
Example:
python
Copy code
from pyspark.sql.functions import regexp_replace
df.withColumn('cleaned_text', regexp_replace(col('text_column'), 'pattern', 'replacement')).show()
67. substring_index
Purpose: Extract a substring from a string column based on a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import substring_index
df.withColumn('substring', substring_index(col('text_column'), 'delimiter', 1)).show()
68. translate
Purpose: Replace characters in a string column with new characters.
Example:
python
Copy code
from pyspark.sql.functions import translate
df.withColumn('translated_text', translate(col('text_column'), 'abc', 'xyz')).show()
69. abs
Purpose: Compute the absolute value of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import abs
df.withColumn('absolute_value', abs(col('numeric_column'))).show()
70. pow
Purpose: Raise a numeric column to a specified power.
Example:
python
Copy code
from pyspark.sql.functions import pow
df.withColumn('squared', pow(col('numeric_column'), 2)).show()
71. sqrt
Purpose: Compute the square root of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import sqrt
df.withColumn('sqrt_value', sqrt(col('numeric_column'))).show()
72. ln / log
Purpose: Compute the natural logarithm or base-10 logarithm of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import log, log10
df.withColumn('log_value', log(col('numeric_column'))).show()
df.withColumn('log10_value', log10(col('numeric_column'))).show()
73. regexp_extract
Purpose: Extract a specific group from a string column using regular expressions.
Example:
python
Copy code
from pyspark.sql.functions import regexp_extract
df.withColumn('extracted', regexp_extract(col('text_column'), 'pattern', 1)).show()
74. current_date
Purpose: Get the current date.
Example:
python
Copy code
from pyspark.sql.functions import current_date
df.withColumn('current_date', current_date()).show()
75. current_timestamp
Purpose: Get the current timestamp.
Example:
python
Copy code
from pyspark.sql.functions import current_timestamp
df.withColumn('current_timestamp', current_timestamp()).show()
76. from_unixtime / unix_timestamp
Purpose: Convert Unix timestamp to a timestamp or vice versa.
Example:
python
Copy code
from pyspark.sql.functions import from_unixtime, unix_timestamp
df.withColumn('timestamp', from_unixtime(col('unix_time_column'))).show()
df.withColumn('unix_time', unix_timestamp(col('timestamp_column'))).show()
77. date_add / date_sub
Purpose: Add or subtract days from a date column.
Example:
python
Copy code
from pyspark.sql.functions import date_add, date_sub
df.withColumn('date_plus_10', date_add(col('date_column'), 10)).show()
df.withColumn('date_minus_10', date_sub(col('date_column'), 10)).show()
78. date_trunc
Purpose: Truncate a date or timestamp to the specified unit.
Example:
python
Copy code
from pyspark.sql.functions import date_trunc
df.withColumn('truncated_date', date_trunc('month', col('date_column'))).show()
79. last_day
Purpose: Get the last day of the month for a given date.
Example:
python
Copy code
from pyspark.sql.functions import last_day
df.withColumn('last_day_of_month', last_day(col('date_column'))).show()
80. window
Purpose: Define window specifications for window functions.
Example:
python
Copy code
from pyspark.sql.window import Window
windowSpec = Window.partitionBy('group_column').orderBy('id')
df.withColumn('row_number', row_number().over(windowSpec)).show()
81. rank / dense_rank
Purpose: Rank rows within a partition.
Example:
python
Copy code
from pyspark.sql.functions import rank, dense_rank
df.withColumn('rank', rank().over(windowSpec)).show()
df.withColumn('dense_rank', dense_rank().over(windowSpec)).show()
82. percent_rank
Purpose: Compute the relative rank of rows in a window.
Example:
python
Copy code
from pyspark.sql.functions import percent_rank
df.withColumn('percent_rank', percent_rank().over(windowSpec)).show()
83. ntile
Purpose: Divide the data into a specified number of buckets.
Example:
python
Copy code
from pyspark.sql.functions import ntile
df.withColumn('quartile', ntile(4).over(windowSpec)).show()
84. covar_pop / covar_samp
Purpose: Compute the population or sample covariance between two columns.
Example:
python
Copy code
from pyspark.sql.functions import covar_pop, covar_samp
df.select(covar_pop(col('col1'), col('col2')).alias('population_covariance')).show()
df.select(covar_samp(col('col1'), col('col2')).alias('sample_covariance')).show()
85. corr
Purpose: Compute the correlation between two columns.
Example:
python
Copy code
from pyspark.sql.functions import corr
df.select(corr(col('col1'), col('col2')).alias('correlation')).show()
86. collect_list / collect_set
Purpose: Aggregate column values into a list or set.
Example:
python
Copy code
from pyspark.sql.functions import collect_list, collect_set
df.groupBy('group_column').agg(collect_list('value').alias('values_list')).show()
df.groupBy('group_column').agg(collect_set('value').alias('values_set')).show()
87. pivot
Purpose: Pivot the data, turning unique values from one column into multiple columns.
Example:
python
Copy code
df.groupBy('group_column').pivot('pivot_column').agg(sum('value')).show()
88. explode_outer
Purpose: Similar to explode, but keeps rows with null arrays.
Example:
python
Copy code
from pyspark.sql.functions import explode_outer
df.withColumn('exploded', explode_outer(col('array_column'))).show()
89. flatten
Purpose: Flatten nested arrays or structs into a single layer.
Example:
python
Copy code
from pyspark.sql.functions import flatten
df.withColumn('flattened', flatten(col('nested_array_column'))).show()
90. bucketBy
Purpose: Create a bucketed table.
Example:
python
Copy code
df.write.bucketBy(4, 'column_name').saveAsTable('bucketed_table')
91. write.format
Purpose: Specify the format for writing data to storage.
Example:
python
Copy code
df.write.format('json').save('output.json')
92. foreachPartition
Purpose: Apply a function to each partition of the DataFrame.
Example:
python
Copy code
def process_partition(iterator):
    for row in iterator:
        print(row)

df.foreachPartition(process_partition)
93. checkpoint
Purpose: Create a checkpoint to truncate the lineage of a DataFrame.
Example:
python
Copy code
df.checkpoint()


94. to_date
Purpose: Convert a string column to a date column.
Example:
python
Copy code
from pyspark.sql.functions import to_date
df.withColumn('date', to_date(col('string_date'), 'yyyy-MM-dd')).show()
95. to_timestamp
Purpose: Convert a string column to a timestamp column.
Example:
python
Copy code
from pyspark.sql.functions import to_timestamp
df.withColumn('timestamp', to_timestamp(col('string_timestamp'), 'yyyy-MM-dd HH:mm:ss')).show()
96. from_utc_timestamp / to_utc_timestamp
Purpose: Convert timestamp between UTC and a specified time zone.
Example:
python
Copy code
from pyspark.sql.functions import from_utc_timestamp, to_utc_timestamp
df.withColumn('local_time', from_utc_timestamp(col('utc_time'), 'America/New_York')).show()
df.withColumn('utc_time', to_utc_timestamp(col('local_time'), 'America/New_York')).show()
97. expr
Purpose: Execute SQL expressions directly.
Example:
python
Copy code
from pyspark.sql.functions import expr
df.withColumn('new_column', expr('col1 + col2')).show()
98. aggregate
Purpose: Perform aggregation operations on columns.
Example:
python
Copy code
from pyspark.sql.functions import sum, avg
df.groupBy('category').agg(sum('amount').alias('total_amount'), avg('amount').alias('avg_amount')).show()
99. sampleBy
Purpose: Sample a DataFrame based on the values of a column.
Example:
python
Copy code
fractions = {'A': 0.1, 'B': 0.2}  # Sample fractions for different categories
df.sampleBy('category', fractions).show()
100. cube
Purpose: Perform cube aggregation for multiple columns.
Example:
python
Copy code
df.cube('category', 'region').agg(sum('amount').alias('total_amount')).show()
101. rollup
Purpose: Perform rollup aggregation for hierarchical levels.
Example:
python
Copy code
df.rollup('category', 'region').agg(sum('amount').alias('total_amount')).show()
102. approxQuantile
Purpose: Compute approximate quantiles of a numeric column.
Example:
python
Copy code
quantiles = df.approxQuantile('amount', [0.25, 0.5, 0.75], 0.01)
print(quantiles)
103. bround
Purpose: Round numeric column to the specified number of decimal places.
Example:
python
Copy code
from pyspark.sql.functions import bround
df.withColumn('rounded_amount', bround(col('amount'), 2)).show()
104. floor / ceil
Purpose: Compute the floor or ceiling value of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import floor, ceil
df.withColumn('floor_amount', floor(col('amount'))).show()
df.withColumn('ceil_amount', ceil(col('amount'))).show()
105. dayofweek / dayofyear
Purpose: Extract day of the week or day of the year from a date column.
Example:
python
Copy code
from pyspark.sql.functions import dayofweek, dayofyear
df.withColumn('day_of_week', dayofweek(col('date_column'))).show()
df.withColumn('day_of_year', dayofyear(col('date_column'))).show()
106. weeks_between
Purpose: Calculate the number of weeks between two dates.
Example:
python
Copy code
from pyspark.sql.functions import weeks_between
df.withColumn('weeks_diff', weeks_between(col('end_date'), col('start_date'))).show()
107. date_format
Purpose: Format a date or timestamp column into a string.
Example:
python
Copy code
from pyspark.sql.functions import date_format
df.withColumn('formatted_date', date_format(col('date_column'), 'yyyy-MM-dd')).show()
108. trunc
Purpose: Truncate a date or timestamp to a specified unit.
Example:
python
Copy code
from pyspark.sql.functions import trunc
df.withColumn('truncated_date', trunc(col('date_column'), 'MM')).show()
109. conv
Purpose: Convert a number from one base to another.
Example:
python
Copy code
from pyspark.sql.functions import conv
df.withColumn('converted', conv(col('hex_column'), 16, 10)).show()
110. aggregate
Purpose: Aggregate values using custom aggregations.
Example:
python
Copy code
from pyspark.sql.functions import expr
df.groupBy('category').agg(expr('sum(amount) as total_amount')).show()
111. ndarray
Purpose: Convert columns to NumPy ndarray (requires Pandas UDFs).
Example:
python
Copy code
from pyspark.sql.functions import pandas_udf
import pandas as pd

@pandas_udf('double')
def to_ndarray(s: pd.Series) -> pd.Series:
    return s.to_numpy()

df.withColumn('ndarray_col', to_ndarray(col('numeric_column'))).show()
112. transform
Purpose: Apply a transformation function to each element in an array column.
Example:
python
Copy code
from pyspark.sql.functions import transform
df.withColumn('transformed_array', transform(col('array_column'), lambda x: x * 2)).show()
113. aggregate
Purpose: Perform aggregation with custom operations.
Example:
python
Copy code
from pyspark.sql.functions import aggregate
df.groupBy('category').agg(aggregate(col('value'), 'sum').alias('total_value')).show()
114. from_avro / to_avro
Purpose: Read and write Avro data.
Example:
python
Copy code
df.write.format('avro').save('output.avro')
df = spark.read.format('avro').load('input.avro')
115. from_json / to_json
Purpose: Read and write JSON data.
Example:
python
Copy code
from pyspark.sql.functions import from_json, to_json
df.withColumn('json_column', to_json(col('struct_column'))).show()
df.withColumn('struct_column', from_json(col('json_column'), schema)).show()
116. writeStream
Purpose: Write a streaming DataFrame to an external sink.
Example:
python
Copy code
df.writeStream.format('console').start().awaitTermination()
117. readStream
Purpose: Read streaming data from an external source.
Example:
python
Copy code
df = spark.readStream.format('json').option('path', 'path/to/directory').load()
118. foreachBatch
Purpose: Apply a function to each batch of a streaming DataFrame.
Example:
python
Copy code
def process_batch(df, epoch_id):
    df.write.format('parquet').mode('append').save('output.parquet')

df.writeStream.foreachBatch(process_batch).start().awaitTermination()
119. checkpoint
Purpose: Create a checkpoint directory to enable fault tolerance for streaming queries.
Example:
python
Copy code
df.writeStream.format('console').option('checkpointLocation', 'path/to/checkpoint').start().awaitTermination()
120. foreach
Purpose: Apply a function to each row in a DataFrame.
Example:
python
Copy code
def print_row(row):
    print(row)

df.foreach(print_row)

121. expr
Purpose: Execute SQL expressions directly.
Example:
python
Copy code
from pyspark.sql.functions import expr
df.withColumn('new_col', expr('col1 + col2')).show()
122. json_tuple
Purpose: Extract multiple values from a JSON string column.
Example:
python
Copy code
from pyspark.sql.functions import json_tuple
df.select(json_tuple(col('json_column'), 'field1', 'field2')).show()
123. struct
Purpose: Create a struct column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import struct
df.withColumn('struct_col', struct(col('col1'), col('col2'))).show()
124. map
Purpose: Create a map from two columns (keys and values).
Example:
python
Copy code
from pyspark.sql.functions import map_from_arrays
df.withColumn('map_col', map_from_arrays(col('keys'), col('values'))).show()
125. transform
Purpose: Apply a transformation function to each element in an array column.
Example:
python
Copy code
from pyspark.sql.functions import transform
df.withColumn('transformed_array', transform(col('array_column'), lambda x: x * 2)).show()
126. slice
Purpose: Extract a portion of an array or string column.
Example:
python
Copy code
from pyspark.sql.functions import slice
df.withColumn('array_slice', slice(col('array_column'), 1, 3)).show()
127. reverse
Purpose: Reverse the order of elements in an array or characters in a string.
Example:
python
Copy code
from pyspark.sql.functions import reverse
df.withColumn('reversed_string', reverse(col('string_column'))).show()
128. trim / ltrim / rtrim
Purpose: Trim whitespace from the start or end of a string.
Example:
python
Copy code
from pyspark.sql.functions import trim, ltrim, rtrim
df.withColumn('trimmed', trim(col('string_column'))).show()
df.withColumn('left_trimmed', ltrim(col('string_column'))).show()
df.withColumn('right_trimmed', rtrim(col('string_column'))).show()
129. when / otherwise
Purpose: Create conditional expressions.
Example:
python
Copy code
from pyspark.sql.functions import when
df.withColumn('new_column', when(col('value') > 10, 'High').otherwise('Low')).show()
130. coalesce
Purpose: Return the first non-null value among its arguments.
Example:
python
Copy code
from pyspark.sql.functions import coalesce
df.withColumn('coalesced', coalesce(col('col1'), col('col2'))).show()
131. nullif
Purpose: Return null if two columns are equal; otherwise, return the first column.
Example:
python
Copy code
from pyspark.sql.functions import nullif
df.withColumn('nullified', nullif(col('col1'), col('col2'))).show()
132. pivot
Purpose: Pivot the data based on a column’s unique values.
Example:
python
Copy code
df.groupBy('category').pivot('month').agg(sum('sales')).show()
133. explode
Purpose: Convert an array or map column into multiple rows.
Example:
python
Copy code
from pyspark.sql.functions import explode
df.withColumn('exploded', explode(col('array_column'))).show()
134. explode_outer
Purpose: Similar to explode, but keeps rows where the array is null.
Example:
python
Copy code
from pyspark.sql.functions import explode_outer
df.withColumn('exploded', explode_outer(col('array_column'))).show()
135. slice
Purpose: Extract a slice from a string or array column.
Example:
python
Copy code
from pyspark.sql.functions import slice
df.withColumn('slice', slice(col('array_column'), 1, 3)).show()
136. array
Purpose: Create an array from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import array
df.withColumn('array_col', array(col('col1'), col('col2'))).show()
137. struct
Purpose: Create a struct column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import struct
df.withColumn('struct_col', struct(col('col1'), col('col2'))).show()
138. map
Purpose: Create a map from arrays of keys and values.
Example:
python
Copy code
from pyspark.sql.functions import map_from_arrays
df.withColumn('map_col', map_from_arrays(col('keys'), col('values'))).show()
139. getField
Purpose: Extract a field from a struct column.
Example:
python
Copy code
from pyspark.sql.functions import col
df.withColumn('field', col('struct_column').getField('field_name')).show()
140. get_json_object
Purpose: Extract a JSON object from a JSON string.
Example:
python
Copy code
from pyspark.sql.functions import get_json_object
df.withColumn('json_field', get_json_object(col('json_column'), '$.field')).show()
141. flatten
Purpose: Flatten nested arrays or structs.
Example:
python
Copy code
from pyspark.sql.functions import flatten
df.withColumn('flattened', flatten(col('nested_array_column'))).show()
142. array_position
Purpose: Find the position of a value in an array column.
Example:
python
Copy code
from pyspark.sql.functions import array_position
df.withColumn('position', array_position(col('array_column'), 'value')).show()
143. array_remove
Purpose: Remove all occurrences of a specified value from an array.
Example:
python
Copy code
from pyspark.sql.functions import array_remove
df.withColumn('cleaned_array', array_remove(col('array_column'), 'value')).show()
144. array_union
Purpose: Compute the union of two arrays.
Example:
python
Copy code
from pyspark.sql.functions import array_union
df.withColumn('union_array', array_union(col('array1'), col('array2'))).show()
145. array_intersect
Purpose: Compute the intersection of two arrays.
Example:
python
Copy code
from pyspark.sql.functions import array_intersect
df.withColumn('intersected_array', array_intersect(col('array1'), col('array2'))).show()
146. array_distinct
Purpose: Remove duplicate elements from an array.
Example:
python
Copy code
from pyspark.sql.functions import array_distinct
df.withColumn('distinct_array', array_distinct(col('array_column'))).show()
147. split
Purpose: Split a string column into an array of substrings based on a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import split
df.withColumn('split_array', split(col('string_column'), ',')).show()
148. element_at
Purpose: Access an element at a specific position in an array or map.
Example:
python
Copy code
from pyspark.sql.functions import element_at
df.withColumn('element', element_at(col('array_column'), 2)).show()
149. expr
Purpose: Execute SQL expressions directly.
Example:
python
Copy code
from pyspark.sql.functions import expr
df.withColumn('new_col', expr('col1 + col2')).show()
150. udf (User Defined Functions)
Purpose: Define and use custom functions in Spark DataFrames.
Example:
python
Copy code
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def my_func(value):
    return value + 1

my_udf = udf(my_func, IntegerType())
df.withColumn('new_column', my_udf(col('existing_column'))).show()



Absolutely, here are even more PySpark functions and methods to further expand your toolkit:

151. lag / lead
Purpose: Access data from previous or subsequent rows in a windowed partition.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import lag, lead

windowSpec = Window.partitionBy('category').orderBy('date')
df.withColumn('prev_amount', lag(col('amount'), 1).over(windowSpec)) \
  .withColumn('next_amount', lead(col('amount'), 1).over(windowSpec)).show()
152. rank / dense_rank / row_number
Purpose: Assign ranks to rows in a partitioned window.
Example:
python
Copy code
from pyspark.sql.functions import rank, dense_rank, row_number
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('category').orderBy('amount')
df.withColumn('rank', rank().over(windowSpec)) \
  .withColumn('dense_rank', dense_rank().over(windowSpec)) \
  .withColumn('row_number', row_number().over(windowSpec)).show()
153. ntile
Purpose: Divide rows in a windowed partition into a specified number of buckets.
Example:
python
Copy code
from pyspark.sql.functions import ntile
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('category').orderBy('amount')
df.withColumn('quartile', ntile(4).over(windowSpec)).show()
154. cume_dist
Purpose: Compute the cumulative distribution of a column value within a window.
Example:
python
Copy code
from pyspark.sql.functions import cume_dist
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('category').orderBy('amount')
df.withColumn('cume_dist', cume_dist().over(windowSpec)).show()
155. percent_rank
Purpose: Compute the relative rank of a value within a partition as a percentage.
Example:
python
Copy code
from pyspark.sql.functions import percent_rank
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('category').orderBy('amount')
df.withColumn('percent_rank', percent_rank().over(windowSpec)).show()
156. first / last
Purpose: Retrieve the first or last value in a windowed partition.
Example:
python
Copy code
from pyspark.sql.functions import first, last
from pyspark.sql.window import Window

windowSpec = Window.partitionBy('category').orderBy('date')
df.withColumn('first_amount', first(col('amount')).over(windowSpec)) \
  .withColumn('last_amount', last(col('amount')).over(windowSpec)).show()
157. pivot
Purpose: Rotate data from rows to columns.
Example:
python
Copy code
df.groupBy('category').pivot('month').agg(sum('sales')).show()
158. unpivot
Purpose: Convert columns into rows (though PySpark does not have a direct unpivot method, this can be done using selectExpr).
Example:
python
Copy code
df.selectExpr("category", "stack(3, 'Jan', Jan, 'Feb', Feb, 'Mar', Mar) as (month, sales)").show()
159. array_contains
Purpose: Check if an array column contains a specified value.
Example:
python
Copy code
from pyspark.sql.functions import array_contains
df.filter(array_contains(col('array_column'), 'value')).show()
160. arrays_zip
Purpose: Combine multiple arrays into an array of structs.
Example:
python
Copy code
from pyspark.sql.functions import arrays_zip
df.withColumn('zipped', arrays_zip(col('array1'), col('array2'))).show()
161. slice
Purpose: Extract a portion of a string or array.
Example:
python
Copy code
from pyspark.sql.functions import slice
df.withColumn('array_slice', slice(col('array_column'), 1, 3)).show()
162. explode_outer
Purpose: Similar to explode, but includes null values.
Example:
python
Copy code
from pyspark.sql.functions import explode_outer
df.withColumn('exploded', explode_outer(col('array_column'))).show()
163. bloom_filter
Purpose: Create a Bloom filter for approximate membership checking (requires pyspark.sql.functions.bloom_filter).
Example:
python
Copy code
from pyspark.sql.functions import bloom_filter
df.withColumn('bloom_filter', bloom_filter(col('column_name'), 0.1)).show()
164. lead / lag
Purpose: Access data from previous or next rows.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag

windowSpec = Window.partitionBy('category').orderBy('date')
df.withColumn('next_value', lead(col('value')).over(windowSpec)) \
  .withColumn('prev_value', lag(col('value')).over(windowSpec)).show()
165. format_number
Purpose: Format a number to a specified number of decimal places.
Example:
python
Copy code
from pyspark.sql.functions import format_number
df.withColumn('formatted', format_number(col('numeric_column'), 2)).show()
166. date_add / date_sub
Purpose: Add or subtract days from a date.
Example:
python
Copy code
from pyspark.sql.functions import date_add, date_sub
df.withColumn('next_date', date_add(col('date_column'), 10)) \
  .withColumn('prev_date', date_sub(col('date_column'), 10)).show()
167. date_trunc
Purpose: Truncate a date or timestamp to a specified unit.
Example:
python
Copy code
from pyspark.sql.functions import date_trunc
df.withColumn('truncated', date_trunc('month', col('date_column'))).show()
168. approxQuantile
Purpose: Compute approximate quantiles of a numeric column.
Example:
python
Copy code
quantiles = df.approxQuantile('amount', [0.25, 0.5, 0.75], 0.01)
print(quantiles)
169. map_from_entries
Purpose: Convert an array of key-value pairs to a map.
Example:
python
Copy code
from pyspark.sql.functions import map_from_entries
df.withColumn('map_col', map_from_entries(col('entries_array'))).show()
170. map_values
Purpose: Extract values from a map column.
Example:
python
Copy code
from pyspark.sql.functions import map_values
df.withColumn('map_values', map_values(col('map_column'))).show()
171. map_keys
Purpose: Extract keys from a map column.
Example:
python
Copy code
from pyspark.sql.functions import map_keys
df.withColumn('map_keys', map_keys(col('map_column'))).show()
172. struct
Purpose: Create a struct column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import struct
df.withColumn('struct_col', struct(col('col1'), col('col2'))).show()
173. array_repeat
Purpose: Repeat a value multiple times in an array.
Example:
python
Copy code
from pyspark.sql.functions import array_repeat
df.withColumn('repeated_array', array_repeat(col('value'), 3)).show()
174. aggregate
Purpose: Aggregate values using a specified function.
Example:
python
Copy code
from pyspark.sql.functions import aggregate
df.groupBy('category').agg(aggregate(col('value'), 'sum').alias('total_value')).show()
175. collect_list / collect_set
Purpose: Collect values into a list or set.
Example:
python
Copy code
from pyspark.sql.functions import collect_list, collect_set
df.groupBy('category').agg(collect_list('value').alias('values_list'),
                           collect_set('value').alias('values_set')).show()
176. stddev_pop / stddev_samp
Purpose: Compute population or sample standard deviation.
Example:
python
Copy code
from pyspark.sql.functions import stddev_pop, stddev_samp
df.agg(stddev_pop(col('value')).alias('stddev_pop'),
       stddev_samp(col('value')).alias('stddev_samp')).show()
177. skewness / kurtosis
Purpose: Compute the skewness or kurtosis of a column.
Example:
python
Copy code
from pyspark.sql.functions import skewness, kurtosis
df.agg(skewness(col('value')).alias('skewness'),
       kurtosis(col('value')).alias('kurtosis')).show()
178. greatest / least
Purpose: Find the greatest or least value from a list of columns.
Example:
python
Copy code
from pyspark.sql.functions import greatest, least
df.withColumn('max_value', greatest(col('col1'), col('col2'))) \
  .withColumn('min_value', least(col('col1'), col('col2'))).show()
179. current_date / current_timestamp
Purpose: Get the current date or timestamp.
Example:
python
Copy code
from pyspark.sql.functions import current_date, current_timestamp
df.withColumn('current_date', current_date()) \
  .withColumn('current_timestamp', current_timestamp()).show()
180. to_date / to_timestamp
Purpose: Convert a string to date or timestamp.
Example:
python
Copy code
from pyspark.sql.functions import to_date, to_timestamp
df.withColumn('date', to_date(col('string_date_column'), 'yyyy-MM-dd')) \
  .withColumn('timestamp', to_timestamp(col('string_timestamp_column'), 'yyyy-MM-dd HH:mm:ss')).show()
181. from_unixtime / unix_timestamp
Purpose: Convert Unix timestamp to string or string to Unix timestamp.
Example:
python
Copy code
from pyspark.sql.functions import from_unixtime, unix_timestamp
df.withColumn('timestamp', from_unixtime(col('unix_timestamp_column'))) \
  .withColumn('unix_time', unix_timestamp(col('timestamp_column'))).show()
182. date_format
Purpose: Format a date or timestamp to a specific string format.
Example:
python
Copy code
from pyspark.sql.functions import date_format
df.withColumn('formatted_date', date_format(col('date_column'), 'yyyy-MM-dd')).show()
183. concat_ws
Purpose: Concatenate multiple columns with a specified delimiter.
Example:
python
Copy code
from pyspark.sql.functions import concat_ws
df.withColumn('concatenated', concat_ws('-', col('col1'), col('col2'))).show()
184. base64 / unbase64
Purpose: Encode or decode a string in base64.
Example:
python
Copy code
from pyspark.sql.functions import base64, unbase64
df.withColumn('base64_encoded', base64(col('string_column'))) \
  .withColumn('base64_decoded', unbase64(col('base64_encoded'))).show()
185. ascii
Purpose: Return the ASCII value of the first character of a string.
Example:
python
Copy code
from pyspark.sql.functions import ascii
df.withColumn('ascii_value', ascii(col('string_column'))).show()
186. concat
Purpose: Concatenate multiple columns or strings.
Example:
python
Copy code
from pyspark.sql.functions import concat
df.withColumn('concatenated', concat(col('col1'), col('col2'))).show()
187. element_at
Purpose: Access an element at a specific position in an array or map.
Example:
python
Copy code
from pyspark.sql.functions import element_at
df.withColumn('element', element_at(col('array_column'), 2)).show()
188. bloom_filter
Purpose: Create a Bloom filter for approximate membership checking (useful in more recent versions).
Example:
python
Copy code
from pyspark.sql.functions import bloom_filter
df.withColumn('bloom_filter', bloom_filter(col('column_name'), 0.1)).show()
189. flatten
Purpose: Flatten nested arrays or structs.
Example:
python
Copy code
from pyspark.sql.functions import flatten
df.withColumn('flattened', flatten(col('nested_array_column'))).show()
190. zip_with
Purpose: Combine elements from multiple arrays using a function.
Example:
python
Copy code
from pyspark.sql.functions import zip_with
df.withColumn('zipped', zip_with(col('array1'), col('array2'), lambda x, y: x + y)).show()
These additional functions cover a wide range of data transformation, manipulation, and aggregation tasks in PySpark, from basic operations to more advanced analytics. If you need further details or have specific use cases, feel free to ask!

need more
ChatGPT
Certainly! Here are even more PySpark functions and methods, expanding on various aspects of data processing and manipulation:

191. aggregate
Purpose: Aggregate data using a specified function and a starting value.
Example:
python
Copy code
from pyspark.sql.functions import aggregate
df.withColumn('sum_value', aggregate(col('array_column'), 0, lambda acc, x: acc + x)).show()
192. countDistinct
Purpose: Count the number of distinct values in a column.
Example:
python
Copy code
from pyspark.sql.functions import countDistinct
df.groupBy('category').agg(countDistinct('value').alias('distinct_count')).show()
193. grouping
Purpose: Return a special column that can be used in GROUP BY queries to determine the level of aggregation.
Example:
python
Copy code
from pyspark.sql.functions import grouping
df.groupBy('category').agg(grouping('category')).show()
194. nanvl
Purpose: Replace NaN values with a specified value.
Example:
python
Copy code
from pyspark.sql.functions import nanvl
df.withColumn('cleaned_value', nanvl(col('value'), 0)).show()
195. arrays_zip
Purpose: Combine multiple arrays into an array of structs.
Example:
python
Copy code
from pyspark.sql.functions import arrays_zip
df.withColumn('zipped_array', arrays_zip(col('array1'), col('array2'))).show()
196. base64 / unbase64
Purpose: Encode or decode data in base64 format.
Example:
python
Copy code
from pyspark.sql.functions import base64, unbase64
df.withColumn('base64_encoded', base64(col('text_column'))) \
  .withColumn('decoded_text', unbase64(col('base64_encoded'))).show()
197. md5 / sha1 / sha2
Purpose: Compute MD5, SHA-1, or SHA-2 hash of a string.
Example:
python
Copy code
from pyspark.sql.functions import md5, sha1, sha2
df.withColumn('md5_hash', md5(col('text_column'))) \
  .withColumn('sha1_hash', sha1(col('text_column'))) \
  .withColumn('sha2_hash', sha2(col('text_column'), 256)).show()
198. lower / upper
Purpose: Convert a string to lower or upper case.
Example:
python
Copy code
from pyspark.sql.functions import lower, upper
df.withColumn('lowercase', lower(col('text_column'))) \
  .withColumn('uppercase', upper(col('text_column'))).show()
199. ascii
Purpose: Return the ASCII value of the first character of a string.
Example:
python
Copy code
from pyspark.sql.functions import ascii
df.withColumn('ascii_value', ascii(col('text_column'))).show()
200. split
Purpose: Split a string into an array of substrings based on a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import split
df.withColumn('split_array', split(col('text_column'), ',')).show()
201. concat_ws
Purpose: Concatenate multiple columns into a single string with a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import concat_ws
df.withColumn('concatenated', concat_ws('-', col('col1'), col('col2'))).show()
202. lit
Purpose: Create a column with a literal value.
Example:
python
Copy code
from pyspark.sql.functions import lit
df.withColumn('constant', lit(100)).show()
203. struct
Purpose: Create a struct column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import struct
df.withColumn('struct_col', struct(col('col1'), col('col2'))).show()
204. slice
Purpose: Extract a portion of a string or array.
Example:
python
Copy code
from pyspark.sql.functions import slice
df.withColumn('slice', slice(col('array_column'), 1, 3)).show()
205. array
Purpose: Create an array column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import array
df.withColumn('array_col', array(col('col1'), col('col2'))).show()
206. element_at
Purpose: Retrieve an element from an array or map at a specified position.
Example:
python
Copy code
from pyspark.sql.functions import element_at
df.withColumn('element', element_at(col('array_column'), 2)).show()
207. explode
Purpose: Transform each element in an array or map into a separate row.
Example:
python
Copy code
from pyspark.sql.functions import explode
df.withColumn('exploded', explode(col('array_column'))).show()
208. flatten
Purpose: Flatten nested arrays or structs.
Example:
python
Copy code
from pyspark.sql.functions import flatten
df.withColumn('flattened', flatten(col('nested_array_column'))).show()
209. explode_outer
Purpose: Similar to explode, but includes rows with null values in the array.
Example:
python
Copy code
from pyspark.sql.functions import explode_outer
df.withColumn('exploded', explode_outer(col('array_column'))).show()
210. array_contains
Purpose: Check if an array contains a specific value.
Example:
python
Copy code
from pyspark.sql.functions import array_contains
df.filter(array_contains(col('array_column'), 'value')).show()
211. map_from_arrays
Purpose: Create a map from two arrays: one for keys and one for values.
Example:
python
Copy code
from pyspark.sql.functions import map_from_arrays
df.withColumn('map_col', map_from_arrays(col('keys_array'), col('values_array'))).show()
212. map_values
Purpose: Extract values from a map.
Example:
python
Copy code
from pyspark.sql.functions import map_values
df.withColumn('map_values', map_values(col('map_column'))).show()
213. map_keys
Purpose: Extract keys from a map.
Example:
python
Copy code
from pyspark.sql.functions import map_keys
df.withColumn('map_keys', map_keys(col('map_column'))).show()
214. bloom_filter
Purpose: Create a Bloom filter for approximate membership checking.
Example:
python
Copy code
from pyspark.sql.functions import bloom_filter
df.withColumn('bloom_filter', bloom_filter(col('column_name'), 0.1)).show()
215. udf (User Defined Functions)
Purpose: Define custom functions for use in DataFrame operations.
Example:
python
Copy code
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def my_func(value):
    return value + 1

my_udf = udf(my_func, IntegerType())
df.withColumn('new_column', my_udf(col('existing_column'))).show()
216. aggregate
Purpose: Apply an aggregation function to a column.
Example:
python
Copy code
from pyspark.sql.functions import aggregate
df.withColumn('sum_value', aggregate(col('array_column'), 0, lambda acc, x: acc + x)).show()
217. nanvl
Purpose: Replace NaN values with a specified value.
Example:
python
Copy code
from pyspark.sql.functions import nanvl
df.withColumn('cleaned_value', nanvl(col('value'), 0)).show()
218. kurtosis
Purpose: Compute the kurtosis of a column.
Example:
python
Copy code
from pyspark.sql.functions import kurtosis
df.agg(kurtosis(col('value')).alias('kurtosis')).show()
219. skewness
Purpose: Compute the skewness of a column.
Example:
python
Copy code
from pyspark.sql.functions import skewness
df.agg(skewness(col('value')).alias('skewness')).show()
220. covar_pop / covar_samp
Purpose: Compute population or sample covariance.
Example:
python
Copy code
from pyspark.sql.functions import covar_pop, covar_samp
df.agg(covar_pop(col('value1'), col('value2')).alias('covar_pop'),
       covar_samp(col('value1'), col('value2')).alias('covar_samp')).show()
These functions and methods extend PySpark’s capabilities in handling complex data processing tasks. If you have specific use cases or need more specialized functions, feel free to ask!

need more
ChatGPT
Sure! Here’s an even more extensive list of PySpark functions and methods for various tasks:

221. pandas_udf (Pandas User Defined Function)
Purpose: Define custom functions using Pandas, allowing operations on chunks of data in parallel.
Example:
python
Copy code
from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import IntegerType

@pandas_udf(IntegerType())
def add_one_udf(v: pd.Series) -> pd.Series:
    return v + 1

df.withColumn('new_column', add_one_udf(col('existing_column'))).show()
222. approxQuantile
Purpose: Compute approximate quantiles of a numeric column.
Example:
python
Copy code
quantiles = df.approxQuantile('value', [0.25, 0.5, 0.75], 0.01)
print(quantiles)
223. monotonically_increasing_id
Purpose: Generate a unique 64-bit integer ID for each row.
Example:
python
Copy code
from pyspark.sql.functions import monotonically_increasing_id
df.withColumn('unique_id', monotonically_increasing_id()).show()
224. hash
Purpose: Compute a hash value for a column.
Example:
python
Copy code
from pyspark.sql.functions import hash
df.withColumn('hashed', hash(col('text_column'))).show()
225. to_avro / from_avro
Purpose: Serialize and deserialize data in Avro format (requires pyspark-avro library).
Example:
python
Copy code
from pyspark.sql.functions import to_avro, from_avro

df.withColumn('avro_data', to_avro(col('struct_column'))) \
  .withColumn('deserialized', from_avro(col('avro_data'))).show()
226. regexp_replace
Purpose: Replace occurrences of a regex pattern in a string column.
Example:
python
Copy code
from pyspark.sql.functions import regexp_replace
df.withColumn('cleaned_text', regexp_replace(col('text_column'), 'pattern', 'replacement')).show()
227. regexp_extract
Purpose: Extract a specific group matched by a regex pattern.
Example:
python
Copy code
from pyspark.sql.functions import regexp_extract
df.withColumn('extracted', regexp_extract(col('text_column'), r'(\d+)', 1)).show()
228. current_date / current_timestamp
Purpose: Get the current date or timestamp.
Example:
python
Copy code
from pyspark.sql.functions import current_date, current_timestamp
df.withColumn('current_date', current_date()) \
  .withColumn('current_timestamp', current_timestamp()).show()
229. date_format
Purpose: Format a date or timestamp column to a specified format.
Example:
python
Copy code
from pyspark.sql.functions import date_format
df.withColumn('formatted_date', date_format(col('date_column'), 'yyyy-MM-dd')).show()
230. date_add / date_sub
Purpose: Add or subtract days from a date column.
Example:
python
Copy code
from pyspark.sql.functions import date_add, date_sub
df.withColumn('date_plus_10', date_add(col('date_column'), 10)) \
  .withColumn('date_minus_10', date_sub(col('date_column'), 10)).show()
231. from_unixtime / unix_timestamp
Purpose: Convert Unix timestamp to a readable date or timestamp and vice versa.
Example:
python
Copy code
from pyspark.sql.functions import from_unixtime, unix_timestamp
df.withColumn('timestamp', from_unixtime(col('unix_time'))) \
  .withColumn('unix_time', unix_timestamp(col('timestamp'))).show()
232. truncate
Purpose: Truncate a timestamp to a specified unit of time.
Example:
python
Copy code
from pyspark.sql.functions import trunc
df.withColumn('truncated_date', trunc(col('date_column'), 'MM')).show()
233. format_number
Purpose: Format a number to a specified number of decimal places.
Example:
python
Copy code
from pyspark.sql.functions import format_number
df.withColumn('formatted', format_number(col('numeric_column'), 2)).show()
234. percent_rank
Purpose: Calculate the relative rank of a value as a percentage within its partition.
Example:
python
Copy code
from pyspark.sql.functions import percent_rank
windowSpec = Window.partitionBy('category').orderBy('value')
df.withColumn('percent_rank', percent_rank().over(windowSpec)).show()
235. approx_count_distinct
Purpose: Compute an approximate count of distinct values in a column.
Example:
python
Copy code
from pyspark.sql.functions import approx_count_distinct
df.agg(approx_count_distinct('value').alias('approx_distinct_count')).show()
236. skewness
Purpose: Compute the skewness of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import skewness
df.agg(skewness(col('value')).alias('skewness')).show()
237. kurtosis
Purpose: Compute the kurtosis of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import kurtosis
df.agg(kurtosis(col('value')).alias('kurtosis')).show()
238. sumDistinct
Purpose: Compute the sum of distinct values in a column.
Example:
python
Copy code
from pyspark.sql.functions import sumDistinct
df.agg(sumDistinct('value').alias('distinct_sum')).show()
239. collect_list / collect_set
Purpose: Collect values into a list or set.
Example:
python
Copy code
from pyspark.sql.functions import collect_list, collect_set
df.groupBy('category').agg(collect_list('value').alias('value_list'),
                           collect_set('value').alias('value_set')).show()
240. map_from_entries
Purpose: Create a map column from an array of key-value pairs.
Example:
python
Copy code
from pyspark.sql.functions import map_from_entries
df.withColumn('map_col', map_from_entries(col('entries_array'))).show()
241. arrays_zip
Purpose: Combine arrays into an array of structs.
Example:
python
Copy code
from pyspark.sql.functions import arrays_zip
df.withColumn('zipped', arrays_zip(col('array1'), col('array2'))).show()
242. flatten
Purpose: Flatten nested arrays or structs.
Example:
python
Copy code
from pyspark.sql.functions import flatten
df.withColumn('flattened', flatten(col('nested_array_column'))).show()
243. bloom_filter
Purpose: Create a Bloom filter for approximate membership checking (in newer versions).
Example:
python
Copy code
from pyspark.sql.functions import bloom_filter
df.withColumn('bloom_filter', bloom_filter(col('column_name'), 0.1)).show()
244. bloom_filter
Purpose: Create a Bloom filter for approximate membership checking (requires pyspark with Avro support).
Example:
python
Copy code
from pyspark.sql.functions import bloom_filter
df.withColumn('bloom_filter', bloom_filter(col('column_name'), 0.1)).show()
245. to_date / to_timestamp
Purpose: Convert a string to date or timestamp.
Example:
python
Copy code
from pyspark.sql.functions import to_date, to_timestamp
df.withColumn('date', to_date(col('string_date_column'), 'yyyy-MM-dd')) \
  .withColumn('timestamp', to_timestamp(col('string_timestamp_column'), 'yyyy-MM-dd HH:mm:ss')).show()
246. date_add / date_sub
Purpose: Add or subtract days from a date column.
Example:
python
Copy code
from pyspark.sql.functions import date_add, date_sub
df.withColumn('date_plus_10', date_add(col('date_column'), 10)) \
  .withColumn('date_minus_10', date_sub(col('date_column'), 10)).show()
247. when / otherwise
Purpose: Implement conditional logic within a DataFrame transformation.
Example:
python
Copy code
from pyspark.sql.functions import when
df.withColumn('new_column', when(col('value') > 10, 'high').otherwise('low')).show()
248. expr
Purpose: Execute SQL expressions within DataFrame operations.
Example:
python
Copy code
from pyspark.sql.functions import expr
df.withColumn('new_column', expr('value * 2')).show()
These functions cover a broad range of data operations, from basic transformations to advanced analytics and custom functionality. If you need more specific examples or have other queries, just let me know!

need more
ChatGPT
Certainly! Here's an extended list of PySpark functions and methods, including some advanced and specialized operations:

249. lead / lag
Purpose: Access data from the following or preceding row in a DataFrame.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import lead, lag

windowSpec = Window.orderBy('date')
df.withColumn('next_value', lead('value').over(windowSpec)) \
  .withColumn('prev_value', lag('value').over(windowSpec)).show()
250. ntile
Purpose: Distribute rows into a specified number of buckets.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import ntile

windowSpec = Window.orderBy('value')
df.withColumn('ntile', ntile(4).over(windowSpec)).show()
251. cume_dist
Purpose: Calculate the cumulative distribution of a column within its partition.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import cume_dist

windowSpec = Window.orderBy('value')
df.withColumn('cumulative_dist', cume_dist().over(windowSpec)).show()
252. percent_rank
Purpose: Compute the percent rank of each row within its partition.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import percent_rank

windowSpec = Window.orderBy('value')
df.withColumn('percent_rank', percent_rank().over(windowSpec)).show()
253. row_number
Purpose: Assign a unique number to each row within its partition.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

windowSpec = Window.orderBy('value')
df.withColumn('row_number', row_number().over(windowSpec)).show()
254. dense_rank
Purpose: Assign ranks to rows within a partition with no gaps between rank values.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import dense_rank

windowSpec = Window.orderBy('value')
df.withColumn('dense_rank', dense_rank().over(windowSpec)).show()
255. rank
Purpose: Assign ranks to rows with gaps in rank values for ties.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

windowSpec = Window.orderBy('value')
df.withColumn('rank', rank().over(windowSpec)).show()
256. first / last
Purpose: Retrieve the first or last value within a partition.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import first, last

windowSpec = Window.partitionBy('category').orderBy('date')
df.withColumn('first_value', first('value').over(windowSpec)) \
  .withColumn('last_value', last('value').over(windowSpec)).show()
257. sum / avg / max / min
Purpose: Compute the sum, average, maximum, or minimum of a column.
Example:
python
Copy code
from pyspark.sql.functions import sum, avg, max, min

df.agg(sum('value').alias('total'),
       avg('value').alias('average'),
       max('value').alias('max_value'),
       min('value').alias('min_value')).show()
258. distinct
Purpose: Remove duplicate rows.
Example:
python
Copy code
df.select('value').distinct().show()
259. dropDuplicates
Purpose: Remove duplicate rows based on specified columns.
Example:
python
Copy code
df.dropDuplicates(['column1', 'column2']).show()
260. fillna
Purpose: Fill NaN values with a specified value.
Example:
python
Copy code
df.fillna({'value': 0, 'category': 'Unknown'}).show()
261. dropna
Purpose: Drop rows with NaN values.
Example:
python
Copy code
df.dropna(subset=['value']).show()
262. replace
Purpose: Replace specific values with new values.
Example:
python
Copy code
df.replace({'old_value': 'new_value'}).show()
263. withColumnRenamed
Purpose: Rename a column.
Example:
python
Copy code
df.withColumnRenamed('old_name', 'new_name').show()
264. selectExpr
Purpose: Select columns using SQL expressions.
Example:
python
Copy code
df.selectExpr('value as new_value', 'category').show()
265. drop
Purpose: Drop columns from a DataFrame.
Example:
python
Copy code
df.drop('column_to_drop').show()
266. select
Purpose: Select specific columns.
Example:
python
Copy code
df.select('column1', 'column2').show()
267. add_months
Purpose: Add a specified number of months to a date column.
Example:
python
Copy code
from pyspark.sql.functions import add_months
df.withColumn('future_date', add_months(col('date_column'), 3)).show()
268. months_between
Purpose: Calculate the number of months between two dates.
Example:
python
Copy code
from pyspark.sql.functions import months_between
df.withColumn('months_diff', months_between(col('date1'), col('date2'))).show()
269. last_day
Purpose: Return the last day of the month for a date column.
Example:
python
Copy code
from pyspark.sql.functions import last_day
df.withColumn('last_day_of_month', last_day(col('date_column'))).show()
270. dayofmonth / dayofweek / dayofyear
Purpose: Extract the day of the month, week, or year from a date column.
Example:
python
Copy code
from pyspark.sql.functions import dayofmonth, dayofweek, dayofyear
df.withColumn('day_of_month', dayofmonth(col('date_column'))) \
  .withColumn('day_of_week', dayofweek(col('date_column'))) \
  .withColumn('day_of_year', dayofyear(col('date_column'))).show()
271. hour / minute / second
Purpose: Extract hour, minute, or second from a timestamp column.
Example:
python
Copy code
from pyspark.sql.functions import hour, minute, second
df.withColumn('hour', hour(col('timestamp_column'))) \
  .withColumn('minute', minute(col('timestamp_column'))) \
  .withColumn('second', second(col('timestamp_column'))).show()
272. year / month / quarter
Purpose: Extract the year, month, or quarter from a date or timestamp column.
Example:
python
Copy code
from pyspark.sql.functions import year, month, quarter
df.withColumn('year', year(col('date_column'))) \
  .withColumn('month', month(col('date_column'))) \
  .withColumn('quarter', quarter(col('date_column'))).show()
273. unix_timestamp
Purpose: Convert a timestamp column to Unix timestamp.
Example:
python
Copy code
from pyspark.sql.functions import unix_timestamp
df.withColumn('unix_time', unix_timestamp(col('timestamp_column'))).show()
274. from_unixtime
Purpose: Convert Unix timestamp to a readable date or timestamp.
Example:
python
Copy code
from pyspark.sql.functions import from_unixtime
df.withColumn('timestamp', from_unixtime(col('unix_time'))).show()
275. first / last
Purpose: Retrieve the first or last value of a column within a window.
Example:
python
Copy code
from pyspark.sql.window import Window
from pyspark.sql.functions import first, last

windowSpec = Window.partitionBy('category').orderBy('date')
df.withColumn('first_value', first('value').over(windowSpec)) \
  .withColumn('last_value', last('value').over(windowSpec)).show()
276. pivot
Purpose: Pivot data from rows to columns.
Example:
python
Copy code
df.groupBy('category').pivot('year').sum('sales').show()
277. explode
Purpose: Convert an array or map into separate rows.
Example:
python
Copy code
from pyspark.sql.functions import explode
df.withColumn('exploded', explode(col('array_column'))).show()
278. posexplode
Purpose: Explode an array or map into separate rows along with positions.
Example:
python
Copy code
from pyspark.sql.functions import posexplode
df.withColumn('exploded', posexplode(col('array_column'))).show()
279. split
Purpose: Split a string column into an array of substrings.
Example:
python
Copy code
from pyspark.sql.functions import split
df.withColumn('split_column', split(col('text_column'), ' ')).show()

249. lit
Purpose: Create a column with a constant literal value.
Example:
python
Copy code
from pyspark.sql.functions import lit
df.withColumn('constant_col', lit(42)).show()
250. col
Purpose: Access a column within a DataFrame for transformations.
Example:
python
Copy code
from pyspark.sql.functions import col
df.select(col('column_name')).show()
251. first
Purpose: Return the first value in a group.
Example:
python
Copy code
from pyspark.sql.functions import first
df.groupBy('category').agg(first('value').alias('first_value')).show()
252. last
Purpose: Return the last value in a group.
Example:
python
Copy code
from pyspark.sql.functions import last
df.groupBy('category').agg(last('value').alias('last_value')).show()
253. approxQuantile
Purpose: Compute approximate quantiles of a numeric column.
Example:
python
Copy code
quantiles = df.approxQuantile('value', [0.25, 0.5, 0.75], 0.01)
print(quantiles)
254. collect_list
Purpose: Collect all values for a column into a list.
Example:
python
Copy code
from pyspark.sql.functions import collect_list
df.groupBy('category').agg(collect_list('value').alias('values_list')).show()
255. collect_set
Purpose: Collect unique values for a column into a set.
Example:
python
Copy code
from pyspark.sql.functions import collect_set
df.groupBy('category').agg(collect_set('value').alias('values_set')).show()
256. explode
Purpose: Transform each element in an array or map into a separate row.
Example:
python
Copy code
from pyspark.sql.functions import explode
df.withColumn('exploded', explode(col('array_column'))).show()
257. explode_outer
Purpose: Similar to explode, but includes rows with null values in the array.
Example:
python
Copy code
from pyspark.sql.functions import explode_outer
df.withColumn('exploded', explode_outer(col('array_column'))).show()
258. struct
Purpose: Create a struct column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import struct
df.withColumn('struct_col', struct(col('col1'), col('col2'))).show()
259. array
Purpose: Create an array column from multiple columns.
Example:
python
Copy code
from pyspark.sql.functions import array
df.withColumn('array_col', array(col('col1'), col('col2'))).show()
260. array_contains
Purpose: Check if an array contains a specific value.
Example:
python
Copy code
from pyspark.sql.functions import array_contains
df.filter(array_contains(col('array_column'), 'value')).show()
261. element_at
Purpose: Retrieve an element from an array or map at a specified position.
Example:
python
Copy code
from pyspark.sql.functions import element_at
df.withColumn('element', element_at(col('array_column'), 2)).show()
262. map_from_entries
Purpose: Create a map from an array of key-value pairs.
Example:
python
Copy code
from pyspark.sql.functions import map_from_entries
df.withColumn('map_col', map_from_entries(col('entries_array'))).show()
263. map_keys
Purpose: Extract keys from a map.
Example:
python
Copy code
from pyspark.sql.functions import map_keys
df.withColumn('map_keys', map_keys(col('map_column'))).show()
264. map_values
Purpose: Extract values from a map.
Example:
python
Copy code
from pyspark.sql.functions import map_values
df.withColumn('map_values', map_values(col('map_column'))).show()
265. bloom_filter
Purpose: Create a Bloom filter for approximate membership checking.
Example:
python
Copy code
from pyspark.sql.functions import bloom_filter
df.withColumn('bloom_filter', bloom_filter(col('column_name'), 0.1)).show()
266. to_avro / from_avro
Purpose: Serialize and deserialize data in Avro format (requires pyspark-avro library).
Example:
python
Copy code
from pyspark.sql.functions import to_avro, from_avro
df.withColumn('avro_data', to_avro(col('struct_column'))) \
  .withColumn('deserialized', from_avro(col('avro_data'))).show()
267. format_number
Purpose: Format a number to a specified number of decimal places.
Example:
python
Copy code
from pyspark.sql.functions import format_number
df.withColumn('formatted', format_number(col('numeric_column'), 2)).show()
268. concat_ws
Purpose: Concatenate multiple columns into a single string with a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import concat_ws
df.withColumn('concatenated', concat_ws('-', col('col1'), col('col2'))).show()
269. concat
Purpose: Concatenate multiple columns or strings.
Example:
python
Copy code
from pyspark.sql.functions import concat
df.withColumn('concatenated', concat(col('col1'), col('col2'))).show()
270. bround
Purpose: Round a numeric column to a specified number of decimal places using Banker's rounding.
Example:
python
Copy code
from pyspark.sql.functions import bround
df.withColumn('rounded', bround(col('numeric_column'), 2)).show()
271. ceil
Purpose: Round a numeric column up to the nearest integer.
Example:
python
Copy code
from pyspark.sql.functions import ceil
df.withColumn('ceiling', ceil(col('numeric_column'))).show()
272. floor
Purpose: Round a numeric column down to the nearest integer.
Example:
python
Copy code
from pyspark.sql.functions import floor
df.withColumn('floor', floor(col('numeric_column'))).show()
273. log / log10
Purpose: Compute the natural logarithm or base-10 logarithm of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import log, log10
df.withColumn('log', log(col('numeric_column'))) \
  .withColumn('log10', log10(col('numeric_column'))).show()
274. exp
Purpose: Compute the exponential of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import exp
df.withColumn('exponential', exp(col('numeric_column'))).show()
275. abs
Purpose: Compute the absolute value of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import abs
df.withColumn('absolute', abs(col('numeric_column'))).show()
276. signum
Purpose: Return the sign of a numeric column.
Example:
python
Copy code
from pyspark.sql.functions import signum
df.withColumn('sign', signum(col('numeric_column'))).show()
277. rand / randn
Purpose: Generate random values. rand generates uniformly distributed values between 0 and 1, while randn generates standard normally distributed values.
Example:
python
Copy code
from pyspark.sql.functions import rand, randn
df.withColumn('random', rand()) \
  .withColumn('random_normal', randn()).show()

  278. bucketBy
Purpose: Define a bucketed table, which can be used to optimize joins and aggregations (requires writing to a table).
Example:
python
Copy code
df.write.bucketBy(4, 'column_name').saveAsTable('bucketed_table')
279. partitionBy
Purpose: Define a partitioned table, which can be used to optimize querying (requires writing to a table).
Example:
python
Copy code
df.write.partitionBy('partition_column').saveAsTable('partitioned_table')
280. coalesce
Purpose: Return the first non-null value in a set of columns.
Example:
python
Copy code
from pyspark.sql.functions import coalesce
df.withColumn('non_null_value', coalesce(col('col1'), col('col2'))).show()
281. nullif
Purpose: Return null if the two specified columns are equal.
Example:
python
Copy code
from pyspark.sql.functions import nullif
df.withColumn('null_if_equal', nullif(col('col1'), col('col2'))).show()
282. isnull / isnotnull
Purpose: Check if a column is null or not null.
Example:
python
Copy code
from pyspark.sql.functions import isnull, isnotnull
df.filter(isnull(col('column_name'))).show()
df.filter(isnotnull(col('column_name'))).show()
283. map
Purpose: Apply a function to each element of a map.
Example:
python
Copy code
from pyspark.sql.functions import map
df.withColumn('mapped_map', map(lambda k, v: (k, v * 2), col('map_column'))).show()
284. transpose
Purpose: Transpose a DataFrame (flip rows and columns).
Example:
python
Copy code
df_transposed = df.selectExpr("stack(3, 'col1', col1, 'col2', col2, 'col3', col3) as (name, value)")
df_transposed.show()
285. split
Purpose: Split a string column into an array based on a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import split
df.withColumn('split_col', split(col('text_column'), 'delimiter')).show()
286. size
Purpose: Get the size of an array or map.
Example:
python
Copy code
from pyspark.sql.functions import size
df.withColumn('array_size', size(col('array_column'))).show()
287. length
Purpose: Get the length of a string column.
Example:
python
Copy code
from pyspark.sql.functions import length
df.withColumn('string_length', length(col('text_column'))).show()
288. substring
Purpose: Extract a substring from a string column.
Example:
python
Copy code
from pyspark.sql.functions import substring
df.withColumn('substring', substring(col('text_column'), 1, 5)).show()
289. repeat
Purpose: Repeat a string a specified number of times.
Example:
python
Copy code
from pyspark.sql.functions import repeat
df.withColumn('repeated', repeat(col('text_column'), 3)).show()
290. translate
Purpose: Replace characters in a string column based on a translation table.
Example:
python
Copy code
from pyspark.sql.functions import translate
df.withColumn('translated', translate(col('text_column'), 'abc', '123')).show()
291. regexp_replace
Purpose: Replace substrings that match a regex pattern.
Example:
python
Copy code
from pyspark.sql.functions import regexp_replace
df.withColumn('cleaned_text', regexp_replace(col('text_column'), 'pattern', 'replacement')).show()
292. regexp_extract
Purpose: Extract substrings matching a regex pattern.
Example:
python
Copy code
from pyspark.sql.functions import regexp_extract
df.withColumn('extracted', regexp_extract(col('text_column'), r'(\d+)', 1)).show()
293. split
Purpose: Split a string column into an array based on a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import split
df.withColumn('split_column', split(col('text_column'), 'delimiter')).show()
294. reverse
Purpose: Reverse the order of characters in a string column.
Example:
python
Copy code
from pyspark.sql.functions import reverse
df.withColumn('reversed', reverse(col('text_column'))).show()
295. concat_ws
Purpose: Concatenate multiple columns with a delimiter.
Example:
python
Copy code
from pyspark.sql.functions import concat_ws
df.withColumn('concatenated', concat_ws('-', col('col1'), col('col2'))).show()
296. uuid
Purpose: Generate a UUID (unique identifier).
Example:
python
Copy code
from pyspark.sql.functions import expr
df.withColumn('uuid', expr('uuid()')).show()
297. date_format
Purpose: Format a date or timestamp column to a specified format.
Example:
python
Copy code
from pyspark.sql.functions import date_format
df.withColumn('formatted_date', date_format(col('date_column'), 'yyyy-MM-dd')).show()
298. from_unixtime / unix_timestamp
Purpose: Convert between Unix timestamps and readable date/timestamp formats.
Example:
python
Copy code
from pyspark.sql.functions import from_unixtime, unix_timestamp
df.withColumn('timestamp', from_unixtime(col('unix_time'))) \
  .withColumn('unix_time', unix_timestamp(col('timestamp'))).show()
299. date_add / date_sub
Purpose: Add or subtract days from a date column.
Example:
python
Copy code
from pyspark.sql.functions import date_add, date_sub
df.withColumn('date_plus_10', date_add(col('date_column'), 10)) \
  .withColumn('date_minus_10', date_sub(col('date_column'), 10)).show()
300. trunc
Purpose: Truncate a timestamp to a specified unit of time (e.g., day, month).
Example:
python
Copy code
from pyspark.sql.functions import trunc
df.withColumn('truncated_date', trunc(col('date_column'), 'MM')).show()

