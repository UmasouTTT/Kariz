#!/bin/sh

GREEN='\033[1;32m'
NC='\033[0m'

echo "${GREEN}Stop History Tracker ${NC}"
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh stop historyserver


echo "${GREEN}Stop Yarn${NC}"
stop-yarn.sh

echo "${GREEN}Stop DFS${NC}"
stop-dfs.sh


echo "${GREEN}Delete tmp floders${NC}"
ansible-playbook delete_hadoop_folder.yml 

echo "${GREEN}Fortmat Namenode${NC}"
hadoop namenode -format

echo "${GREEN}Start DFS${NC}"
start-dfs.sh

echo "${GREEN}Start Yarn${NC}"
start-yarn.sh

echo "${GREEN}Start History Tracker${NC}"
$HADOOP_HOME/sbin/mr-jobhistory-daemon.sh start historyserver

#echo "${GREEN}Add App directory${NC}"
#hadoop fs -mkdir /apps/

#echo "${GREEN}Add Tez directory${NC}"
#hadoop fs -mkdir /apps/tez-0.9.0

#echo "${GREEN}Copy Tez file${NC}"
#hadoop fs -copyFromLocal /home/ubuntu/opt/apache-tez-0.9.0-src/tez-dist/target/tez-0.9.0.tar.gz /apps/tez-0.9.0/
