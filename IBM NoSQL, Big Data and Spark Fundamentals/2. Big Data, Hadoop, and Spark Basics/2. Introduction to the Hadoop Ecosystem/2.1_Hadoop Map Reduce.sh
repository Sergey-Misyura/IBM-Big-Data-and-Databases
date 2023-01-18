#!/bin/bash

curl https://dlcdn.apache.org/hadoop/common/hadoop-3.2.3/hadoop-3.2.3.tar.gz --output hadoop-3.2.3.tar.gz

tar -xvf hadoop-3.2.3.tar.gz

cd hadoop-3.2.3

bin/hadoop

bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-3.2.3.jar wordcount data.txt output

ls output

cat  output/part-r-00000