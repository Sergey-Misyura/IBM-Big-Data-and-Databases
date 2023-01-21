#!/bin/bash

# Prerequisites
# Note: If you are running this lab within the Skillsnetwort Lab environment, all prerequisites are already installed for you
# The only pre-requisites to this lab are:
# - A working docker installation
# - Docker Compose
# - The git command line tool
# - A python development environment

python3 -m pip install pyspark
git clone https://github.com/big-data-europe/docker-spark.git
cd docker-spark
docker-compose up
