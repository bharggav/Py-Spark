# Py-Spark
installation of spark
step by step in 

or 

Docker platform  is required in windows

Once you have docker in your system you can below command in CMD

docker pull spark

docker run -p 4040:4040 -it spark:python3 /opt/spark/bin/pyspark

docker ps 

docker start "spark_container_name"

docker exec -it "spark_container_name" /bin/bash
/opt/spark/bin/pyspark

Example:- once container started 
Step 1:- run below command

docker exec -it c5d83632cb6f4ac410f141a4078b6c907147cca1d68f337cab0881b8d9f45527 /bin/bash

Step 2:- run below command

/opt/spark/bin/pyspark

