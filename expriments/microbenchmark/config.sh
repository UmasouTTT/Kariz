#!/bin/bash

export rgw_host="192.168.35.41"
export rgw_port=80
export swift_user="testuser:swift"
export swift_key="7Xqb6gdsCE5Vu0clmk2qL0yjjy1NCNiFuaPlGQvJ"
export bucket_name="data"
export cache_block_size=4194304 # 4 MB
export reps=3
#export app_name="dfsio"
export app_name="wordcount"
export hibench_root="/local0/HiBench"
export dpath="HiBench/Wordcount/Input-32G"
export hibench_command="bin/workloads/micro/wordcount/hadoop/run.sh"
#export dpath="HiBench/Dfsioe/Input/io_data/"
#export hibench_command="bin/workloads/micro/dfsioe/hadoop/run_read.sh"
export grace_time=40
export report_file="wordcount_1gbps.csv"
