#!/bin/bash

# Prerequisites
# Note: If you are running this lab within the Skillsnetwort Lab environment, all prerequisites are already installed for you
# The only pre-requisites to this lab are:
# - A working docker installation
# - Docker Compose
# - The git command line tool
# - A python development environment

#Setup
git clone https://github.com/ibm-developer-skills-network/fgskh-new_horizons.git
cd fgskh-new_horizons
alias k='kubectl'
my_namespace=$(kubectl config view --minify -o jsonpath='{..namespace}')

#Deploy the Apache Spark Kubernetes Pod
k apply -f spark/pod_spark.yaml
k get po
#k delete po spark #to delete the pod and install again

#Submit Apache Spark jobs to Kubernetes
k exec spark -c spark  -- echo "Hello from inside the container"
k exec spark -c spark -- ./bin/spark-submit \
--master k8s://http://127.0.0.1:8001 \
--deploy-mode cluster \
--name spark-pi \
--class org.apache.spark.examples.SparkPi \
--conf spark.executor.instances=1 \
--conf spark.kubernetes.container.image=romeokienzler/spark-py:3.1.2 \
--conf spark.kubernetes.executor.request.cores=0.2 \
--conf spark.kubernetes.executor.limit.cores=0.3 \
--conf spark.kubernetes.driver.request.cores=0.2 \
--conf spark.kubernetes.driver.limit.cores=0.3 \
--conf spark.driver.memory=512m \
--conf spark.kubernetes.namespace=${my_namespace} \
local:///opt/spark/examples/jars/spark-examples_2.12-3.1.2.jar \
10

#Monitor the Spark application in a parallel terminal
kubectl get po
#replace driver id
kubectl logs spark-pi-6f62d17a800beb3e-driver |grep "Job 0 finished:" 
kubectl logs spark-pi-6f62d17a800beb3e-driver |grep "Pi is roughly " 