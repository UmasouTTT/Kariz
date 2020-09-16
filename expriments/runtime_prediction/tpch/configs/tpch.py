#!/usr/bin/python3

config_bw_playbook = "/local0/Kariz/scripts/setup_tools/config_ceph_bw.yml"
clear_spark_playbook = "/local0/Kariz/scripts/setup_tools/delete_spark_folder.yml"
restart_rgw_playbook = "/local0/Kariz/scripts/setup_tools/restart_rgw_remote.yml"

rgw_nic='ens2f1'
rgw_rates=[ '10Gbps', '20Gbps', '500Mbps', '40Gbps', '1Gbps', '5Gbps']
#rgw_rates=[ '40Gbps', '1Gbps', '5Gbps']



rgw_host='192.168.37.41'
rgw_port=80
swift_user = 'testuser:swift'
swift_key = '7Xqb6gdsCE5Vu0clmk2qL0yjjy1NCNiFuaPlGQvJ'
bucket_name='data'
cache_block_size = 4194304 # 4 MB

n_maps = 64
prefered_map_size=512*1024*1024


output_path="/tpch/output"
input_path='s3a://%s/pig-tpch/'%(bucket_name)
datasets = ['20G', '32G', '64G']
tables= ['customer', 'lineitem', 'nation', 
        'orders', 'part', 'partsupp', 
        'region', 'supplier']

#strides=[64, 128, 64, 8, 16, 32, 48, 4, 80, 96]
strides = [10, 20, 30, 40, 50, 60, 70, 80, 90, 100, 0]
#datasets=['64G']
#strides=[64,128,0]

framework='pig'
app_name="tpch"

benchmark_root="/local0/Kariz/expriments/benchmark/BenchmarkScripts/tpch/pig"
executable="%s/run_tpch.sh"%(benchmark_root)
executable='%s/run_tpch_query.sh'%(benchmark_root)

repeats=3

statfile="/local0/Kariz/expriments/runtime_prediction/tpch/results/%s-%s.csv"%(app_name,framework)
