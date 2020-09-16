#!/bin/sh

GREEN='\033[1;32m'
NC='\033[0m'

echo "${GREEN}Copy Dataset to S3 ${NC}"
hadoop fs -cp /tpch s3a://alluxio/alluxio


#echo "${GREEN}Copy Dataset to Alluxio${NC}"
#hadoop fs -cp /user/root/tpch alluxio://neu-5-1:19998/

