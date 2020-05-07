#!/bin/bash


ip_forward=`cat /proc/sys/net/ipv4/ip_forward`

if [ "${ip_forward}" -eq "0" ]; then
   echo 1 > /proc/sys/net/ipv4/ip_forward
fi


port=$1

echo "Enable port forwarding on ${port}"


iptables -t nat -A PREROUTING -p tcp -d 10.255.5.1 --dport ${port} -j DNAT --to 192.168.37.1:${port}
iptables -A FORWARD -p tcp -d 192.168.37.1 --dport ${port} -j ACCEPT



