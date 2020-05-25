rgw_host='192.168.35.41'
rgw_port=80
swift_user = 'testuser:swift'
swift_key = '7Xqb6gdsCE5Vu0clmk2qL0yjjy1NCNiFuaPlGQvJ'
bucket_name='data'
cache_block_size = 4194304 # 4 MB
reps = 2
app_name='Q3'
experiment_name=['KARIZ','MRD','CP','NoCache','Infinite']
dpath = ['pig-tpch/64G/lineitem,pig-tpch/64G/orders,pig-tpch/64G/customer,pig-tpch/64G/supplier,pig-tpch/64G/region,pig-tpch/64G/nation',
        'pig-tpch/64G/lineitem,pig-tpch/64G/orders,pig-tpch/64G/customer,pig-tpch/64G/supplier,pig-tpch/64G/region,pig-tpch/64G/nation',
        'pig-tpch/64G/lineitem,pig-tpch/64G/orders,pig-tpch/64G/customer,pig-tpch/64G/supplier,pig-tpch/64G/region,pig-tpch/64G/nation',
        'pig-tpch/64G/lineitem,pig-tpch/64G/orders,pig-tpch/64G/customer,pig-tpch/64G/supplier,pig-tpch/64G/region,pig-tpch/64G/nation',
        'pig-tpch/64G/lineitem,pig-tpch/64G/orders,pig-tpch/64G/customer,pig-tpch/64G/supplier,pig-tpch/64G/region,pig-tpch/64G/nation']

strides=['55,-1,0,0,0,-1', '0,-1,-1,0,0,-1', '0,0,0,0,0,-1', '0,0,0,0,0,0', '-1,-1,-1,-1,0,-1']
#dpath='HiBench/Dfsioe/Input/io_data/'
hibench_root='/local0/HiBench'
hibench_command='bin/workloads/micro/wordcount/hadoop/run.sh'
#hibench_command="bin/workloads/micro/dfsioe/hadoop/run_read.sh"
grace_time=30
report_file="aon_vs_partial.csv"
