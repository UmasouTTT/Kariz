#!/bin/sh

GREEN='\033[1;32m'
NC='\033[0m'

echo "${GREEN}Stop Alluxio ${NC}"
alluxio-stop.sh all
#$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver


echo "${GREEN}Create Alluxio UnderFS directory${NC}"
hadoop fs -mkdir /alluxio;
hadoop fs -mkdir /alluxio/data;

#echo "${GREEN}Validate Alluxio Environment${NC}"
#${ALLUXIO_HOME}/bin/alluxio validateEnv all

echo "${GREEN}Format Alluxio${NC}"
${ALLUXIO_HOME}/bin/alluxio format


echo "${GREEN}Start Alluxio${NC}"
${ALLUXIO_HOME}/bin/alluxio-start.sh all

