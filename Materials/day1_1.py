# Import SparkSession
from pyspark.sql import SparkSession

# Create SparkSession 
spark = SparkSession.builder.master("local[1]").appName("test1").getOrCreate() 

##if session is stared active session will be there we need to sopt the session and recreate it.

from pyspark.sql import SparkSession

# Check if a SparkSession already exists
if not SparkSession.getActiveSession():
    spark = SparkSession.builder.master("local[1]").appName("test1").getOrCreate()
else:
    spark = SparkSession.getActiveSession()


or 

from pyspark.sql import SparkSession

# Stop any existing SparkSession
spark = SparkSession.builder.master("local[1]").appName("test1").getOrCreate()
spark.stop()

# Create a new SparkSession
spark = SparkSession.builder.master("local[1]").appName("test1").getOrCreate()
