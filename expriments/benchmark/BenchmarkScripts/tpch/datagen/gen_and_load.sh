unzip -n tpch_data_gen.zip
perl tpch_gen_data.pl data.properties
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/lineitem.tbl* s3a://testbucket/tpch/d56/lineitem
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/orders.tbl*   s3a://testbucket/tpch/d56/orders
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/customer.tbl* s3a://testbucket/tpch/d56/customer
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/partsupp.tbl* s3a://testbucket/tpch/d56/partsupp
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/part.tbl*     s3a://testbucket/tpch/d56/part
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/supplier.tbl* s3a://testbucket/tpch/d56/supplier
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/nation.tbl*   s3a://testbucket/tpch/d56/nation
/root/hadoop_modified/hadoop-2.10.1/bin/hadoop fs -put data/region.tbl*   s3a://testbucket/tpch/d56/region
rm -rf data/*.tbl*
