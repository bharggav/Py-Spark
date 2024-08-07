# Import SparkSession
from pyspark.sql import SparkSession

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

df.show()

(or)

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

# Retrieve the data from the temporary view
result_df = spark.table("employee")

# Show the first few rows of the result
result_df.show()

