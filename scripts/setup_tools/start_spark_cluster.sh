#!/bin/sh

GREEN='\033[1;32m'
NC='\033[0m'

echo "${GREEN}Stop History Tracker ${NC}"
/opt/spark-3.0.0/sbin/stop-history-server.sh


echo "${GREEN}Stop Spark workers${NC}"
$SPARK_HOME/sbin/stop-all.sh


echo "${GREEN}Start Spark worker${NC}"
$SPARK_HOME/sbin/start-all.sh

hadoop fs -rm -r /local0/
hadoop fs -mkdir /local0
hadoop fs -mkdir /local0/spark-logs

echo "${GREEN}Start History Tracker${NC}"
/opt/spark-3.0.0/sbin/start-history-server.sh 

