#!/bin/bash

docker cp ../data/ namenode:/home

# docker cp . namenode:/home

docker exec -it namenode bash -c "hdfs dfsadmin -safemode leave"
docker exec -it namenode bash -c "hdfs dfs -rm -r -f /data*"


docker exec -it namenode bash -c "hdfs dfs -mkdir /data"

docker exec -it namenode bash -c "hdfs dfs -put /home/data/* /data"