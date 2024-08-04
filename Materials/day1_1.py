import os
from pyspark.sql import SparkSession

# Set the path to Python executable
os.environ['PYSPARK_PYTHON'] = r'C:\Users\bharg\AppData\Local\Programs\Python\Python35-32\python.exe'  
# Update with your Python path

# Create SparkSession
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("test1") \
    .config("spark.pyspark.python", os.environ['PYSPARK_PYTHON']) \
    .getOrCreate()

# Create DataFrame
data = [('James', '', 'Smith', '1991-04-01', 'M', 3000),
        ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', '1978-09-05', 'M', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]
columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]

df = spark.createDataFrame(data=data, schema=columns)
df.show()
