#!/bin/bash

spark-submit --conf spark.sql.crossJoin.enabled=true --conf spark.sql.autoBroadcastJoinThreshold=-1 --master spark://neu-5-1:7077 --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark build/libs/spark-tpcds.jar --tpcds-data-location s3a://data/tpcds-par-2g/ --out-data-location hdfs://neu-5-1:9000/tmp/output --query-filter q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14

spark-submit --conf spark.sql.crossJoin.enabled=true --conf spark.sql.autoBroadcastJoinThreshold=-1 --master spark://neu-5-1:7077 --class org.apache.spark.sql.execution.benchmark.TPCDSQueryBenchmark build/libs/spark-tpcds.jar --tpcds-data-location s3a://data/tpcds-par-2g/ --out-data-location hdfs://neu-5-1:9000/tmp/output-cache --query-filter q1,q2,q3,q4,q5,q6,q7,q8,q9,q10,q11,q12,q13,q14
