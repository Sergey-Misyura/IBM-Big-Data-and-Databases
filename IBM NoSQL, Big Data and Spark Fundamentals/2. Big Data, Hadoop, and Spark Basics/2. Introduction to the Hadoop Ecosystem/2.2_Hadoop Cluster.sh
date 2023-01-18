#!/bin/bash

git clone https://github.com/ibm-developer-skills-network/ooxwv-docker_hadoop.git

cd ooxwv-docker_hadoop

docker-compose up -d

docker exec -it namenode /bin/bash

ls /opt/hadoop-3.2.1/etc/hadoop/*.xml

hdfs dfs -mkdir -p /user/root/input

hdfs dfs -put $HADOOP_HOME/etc/hadoop/*.xml /user/root/input

curl https://raw.githubusercontent.com/ibm-developer-skills-network/ooxwv-docker_hadoop/master/SampleMapReduce.txt --output data.txt 

hdfs dfs -put data.txt /user/root/

hdfs dfs -cat /user/root/data.txt
