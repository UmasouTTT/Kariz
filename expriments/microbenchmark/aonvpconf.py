rgw_host='192.168.35.41'
rgw_port=80
swift_user = 'testuser:swift'
swift_key = '7Xqb6gdsCE5Vu0clmk2qL0yjjy1NCNiFuaPlGQvJ'
bucket_name='data'
cache_block_size = 4194304 # 4 MB
reps = 3
app_name='wordcount'
experiment_name=['allnothing', 'partial', 'remote', 'allcached']
dpath = ['HiBench/Wordcount/Input-32G-2', 'HiBench/Wordcount/Input-32G-1,HiBench/Wordcount/Input-32G-2,HiBench/Wordcount/Input-32G-3',
        'HiBench/Wordcount/Input-32G-2', 'HiBench/Wordcount/Input-32G-1,HiBench/Wordcount/Input-32G-2,HiBench/Wordcount/Input-32G-3']
strides=['256', '128,128,256', '0', '256,256,256']
#dpath='HiBench/Dfsioe/Input/io_data/'
hibench_root='/local0/HiBench'
hibench_command='bin/workloads/micro/wordcount/hadoop/run.sh'
#hibench_command="bin/workloads/micro/dfsioe/hadoop/run_read.sh"
grace_time=30
report_file="aon_vs_partial.csv"
