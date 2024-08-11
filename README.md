# Py-Spark
installation of spark
step by step in 

or 

Docker platform  is required in windows

Once you have docker in your system you can below command in CMD

docker pull spark

docker run --name PySpark -p 4040:4040 -it spark:python3 /opt/spark/bin/pyspark

docker ps 

docker start "spark_container_name"

docker exec -it "spark_container_name" /bin/bash
/opt/spark/bin/pyspark

Example:- once container started 
Step 1:- run below command

docker exec -it 24bdf6d2f3a83230b3d7589176f48c56ce9698332102a04e6896fb3a10dbaf57 /bin/bash

Step 2:- run below command

/opt/spark/bin/pyspark

