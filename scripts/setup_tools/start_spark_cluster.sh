#!/bin/sh

GREEN='\033[1;32m'
NC='\033[0m'

echo "${GREEN}Stop History Tracker ${NC}"
$SPARK_HOME/sbin/stop-history-server.sh


echo "${GREEN}Stop Spark workers${NC}"
$SPARK_HOME/sbin/stop-all.sh


echo "${GREEN}Start Spark worker${NC}"
$SPARK_HOME/sbin/start-all.sh


echo "${GREEN}Start History Tracker${NC}"
$SPARK_HOME/sbin/start-history-server.sh 

