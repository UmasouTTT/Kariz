#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Usage: enable_portforwarding.sh <public_ip> <private_ip>"
    exit
fi

src=$1
dest=$2


echo "Enable Hadoop cluster webui"
./enable_portforwarding.sh ${src} ${dest} 8088

echo "Enable Hadoop history server webui"
./enable_portforwarding.sh ${src} ${dest} 19888

echo "Enable Hadoop HDFS webui"
./enable_portforwarding.sh ${src} ${dest} 50070

echo "Enable Hive webui"
./enable_portforwarding.sh ${src} ${dest} 10002

echo "Enable Spark webui"
./enable_portforwarding.sh ${src} ${dest} 8080

echo "Enable Spark webui"
./enable_portforwarding.sh ${src} ${dest} 4040

echo "Enable Spark Job history webui"
./enable_portforwarding.sh ${src} ${dest} 18080


