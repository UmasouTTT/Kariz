#!/bin/bash

if [ "$#" -ne 3 ]; then
    echo "Usage: enable_portforwarding.sh <public_ip> <private_ip> <port>"
    exit
fi


ip_forward=`cat /proc/sys/net/ipv4/ip_forward`

if [ "${ip_forward}" -eq "0" ]; then
   sudo bash -c "echo 1 > /proc/sys/net/ipv4/ip_forward"
fi


SRC=$1
DEST=$2
port=$3

echo "Enable port forwarding on ${SRC}, ${DEST}, ${port}"


iptables -t nat -A PREROUTING -p tcp -d ${SRC} --dport ${port} -j DNAT --to ${DEST}:${port}
iptables -A FORWARD -p tcp -d ${DEST} --dport ${port} -j ACCEPT


