#!/usr/bin/python3

config_bw_playbook = "/local0/Kariz/scripts/setup_tools/config_ceph_bw.yml"
clear_spark_playbook = "/local0/Kariz/scripts/setup_tools/delete_spark_folder.yml"
restart_rgw_playbook = "/local0/Kariz/scripts/setup_tools/restart_rgw_remote.yml"

rgw_nic='ens2f1'
#rgw_rates=['500Mbps', '100Mbps', '40Gbps', '1Gbps', '5Gbps', '10Gbps', '20Gbps']
rgw_rates=['500Mbps', '100Mbps', '40Gbps']



rgw_host='192.168.37.41'
rgw_port=80
swift_user = 'testuser:swift'
swift_key = '7Xqb6gdsCE5Vu0clmk2qL0yjjy1NCNiFuaPlGQvJ'
bucket_name='data'
cache_block_size = 4194304 # 4 MB

n_maps = 64
prefered_map_size=512*1024*1024


output_path="/HiBench/Wordcount/Output"
input_path='s3a://%s/HiBench/Wordcount/Input-'%(bucket_name)
datasets = ['32G', '64G', '16G', '4G', '1G']
strides=[64, 128, 8, 16, 32, 48, 4, 80, 96, 0]

#datasets=['64G']
#strides=[64,128,0]

framework='spark'
app_name="wordcount"

benchmark_root="/local0/Kariz/expriments/benchmark/HiBench"
executable="%s/bin/workloads/micro/%s/%s/run2.sh"%(benchmark_root, app_name, framework)

repeats=3

statfile="/local0/Kariz/expriments/runtime_prediction/micro/results/%s-%s.csv"%(app_name,framework)
