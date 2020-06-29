#!/bin/bash

echo "Hard restart Radosgw"

cur_dir=$PWD
rgw_path="/home/amin/Ceph-RGW-Prefetching/build/"
rgw_data_path="/tmp"

rgw_pid=$(ps ax | grep radosgw | awk 'NR==1{if ($5=="./bin/radosgw") print $1}')

if [[ ${rgw_pid} ]]; then
   echo "Kill Radosgw with pid:$rgw_pid"
   kill -9 $rgw_pid
fi

# Delete _2* from tmp directory
echo "Delete all ${rgw_data_path}/*_2"
rm -rf "${rgw_data_path}/*_2"

# Delete _1* from tmp directory
echo "Delete all ${rgw_data_path}/*_1"
rm -rf "${rgw_data_path}/*_1"

# Delete everything in tmp directory
echo "Delete all files in ${rgw_data_path}"
rm -rf "${rgw_data_path}/*"

cd ${rgw_path}

echo "Start rgw"
./bin/radosgw -n client.rgw.neu-3-41 -c /etc/ceph/ceph.conf

cd ${cur_dir}
