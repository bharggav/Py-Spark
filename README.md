# Py-Spark intsallation 3 Methods

### Method 1:- 
installation of spark step by step in  Apache Spark Installation.docx

### Method 2:-official image from apache/spark

Docker platform is required in windows Operating system Once you have docker in your system you can run below command in CMD

## ***docker pull spark*** 
# To Run the container:-
## ***docker run --name PySpark -p 4040:4040 -it spark:python3 /opt/spark/bin/pyspark*** #Directly starts the pyspark shell 
System is shutdow your container has stoped then follow below steps
## ***docker ps (or) docker ps -a*** 
## ***docker start "spark_container_name(or)container_id"***
## ***docker exec -it "spark_container_name(or)container_id" /bin/bash***
## ***/opt/spark/bin/pyspark***
Example:- once container started 
Step 1:- run below command
***docker exec -it 24bdf6d2f3a83230b3d7589176f48c56ce9698332102a04e6896fb3a10dbaf57 /bin/bash***
Step 2:- run below command
***/opt/spark/bin/pyspark***

Reference link:- https://hub.docker.com/r/apache/spark-py

### Method 3:- By me

Docker platform is required in windows Operating system Once you have docker in your system you can run below command in CMD

## ***docker pull bharggavgalla/pyspark***  
## ***docker run -it -p 4040:4040 -p 18080:18080 --name pyspark-container bharggavgalla/pyspark***
System is shutdow your container has stoped then follow below steps
## ***docker ps (or) docker ps -a*** 
## ***docker start "spark_container_name(or)container_id"***
## ***docker exec -it "spark_container_name(or)container_id" /bin/bash***
## ***pyspark***
Reference link:- https://hub.docker.com/r/bharggavgalla/pyspark