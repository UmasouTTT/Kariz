How to Run:
   To generate dataset
	1. cd ~/papers/KARIZ/expriments/benchmark/BenchmarkScripts/tpch/datagen/ 
        2. to generate data
                perl gen_data.pl <a> <b> <c> <d> <address of dataset locally> <address of dataset in HDFS>
                    where 
                        <a> is dataset size in GB
                        <b> is number of generated files
                        <c> is zipf factor which we always set to 0.
                        <d> list of hosts that participate in generating data. Look at any .txt file in this folder for examples
                        <e> the folder to store generated data temporarily on each host, it should be the same on all nodes. for example `/home/mania/gen_data`
                        <f> the address in HDFS to store data. for example `/tpch/in`. In this example the `/tpch` folder must be created before but the benchmark 
                            creates the `in` folder within `/tpch` during data generation. 
                         
    To run PIG scripts:
         1. cd /root/papers/KARIZ/expriments/benchmark/BenchmarkScripts/tpch/pig 
         2. use run_full_benchmark to run all of 22 queries for Alluxio, HDFS and S3. 
          	1. Modify run_full_benchmark to change the dataset addresses in Alluxio, HDFS and S3 for input data. 
          	2. Modify run_full_benchmark to change the number of times we run all of the 22 queries for that dataset. This is the second option in the run_full_benchmark.sh commands.

    To copy the generated data from HDSF to S3 and alluxio use
	1. copy_dataset_to_alluxio_s3.sh and modify addresses within this file with your desired addresses. 
    
     To run for each baclend (S3, HDFS, Alluxio):
    	3. look at the ReadMe at /root/papers/KARIZ/expriments/benchmark/BenchmarkScripts/tpch/pig 
         
   
To start Alluxio iand Hadoop cluster:
    1. cd /root/papers/KARIZ/scripts/setup_tools and from there run ./start_alluxio_cluster. Remember to modify files in $ALLUXIO_HOME/etc/conf
    2. from the same folder run ./start_hadoop_cluster and modify the proper configuration. 
       

How to check results:




