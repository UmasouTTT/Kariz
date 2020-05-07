#!/bin/bash

echo "Enable Hadoop cluster webui"
./enable_portforwarding.sh 8088

echo "Enable Hadoop history server webui"
./enable_portforwarding.sh 19888

echo "Enable Hadoop HDFS webui"
./enable_portforwarding.sh 50070

echo "Enable Hive webui"
./enable_portforwarding.sh 10002

echo "Enable Spark webui"
./enable_portforwarding.sh 8080


