#!/bin/sh

GREEN='\033[1;32m'
NC='\033[0m'

#echo "${GREEN} Run Input Alluxio, Output HDFS (local)${NC}"
#./benchmark_tpch.sh 4 alluxio://neu-5-1:19998/tpch/in /user/root/tpch/out_alx 1 ALLUXIO HDFS | tee ALLUXIO_HDFS_8wrk.log

echo "${GREEN} Run Input S3, Output HDFS (local)${NC}"
./benchmark_tpch.sh 5 s3a://data/tpch-1G /tpch/q6_1g_5rep_s3 1 S3 HDFS | tee results/q6_1g_5rep_s3.log

#echo "${GREEN} Run Input HDFS, Output HDFS (local)${NC}"
#./benchmark_tpch.sh 1 /tpch/tpch/in /user/root/tpch/out_4rep_hdfs4 1 HDFS HDFS | tee HDFS_HDFS_4wrk_1rep.log

#echo "${GREEN} Run Input Alluxio, Output HDFS (local)${NC}"
#./benchmark_tpch.sh 1 alluxio://neu-5-1:19998/tpch/in /user/root/tpch/out_4rep_alx 1 ALLUXIO HDFS | tee ALLUXIO_HDFS_4wrk_4rep.log
