#!/usr/bin/python3
'''
Created on Sep 16, 2019

@author: mania
'''
import gateway
import datetime
import json
from colorama import Fore, Style
import utils.requester as req


#workload.load_graphs('config/synthetic_dags_2_1_sharing.g')
#workload.start_experiment()
def load_synthetic_stream_graphs(fpath):
    runtime_stats = {}
    with open(fpath, 'r') as fd:
        wstr = fd.read()
        workload = gateway.Workload()
        start_time = datetime.datetime.now()
        workload.load_graphs_fromstring(wstr)
        runtime_stats, finish_time = workload.start_experiment()
        runtime_stats['exec_time'] = finish_time
    print(Fore.RED, 'End-to-end experiment runtime %d, simultation time %d'%(finish_time, (datetime.datetime.now() - start_time).total_seconds()), Style.RESET_ALL)

#    with open('mdmc_sw_zipf_spark_80p_512_isolated.json', 'w') as fd:
#        fd.write(json.dumps(runtime_stats))
    with open('mc_sw_zipf_spark_20p_1Tcmr.json', 'w') as fd:
        fd.write(json.dumps(runtime_stats))
    req.send_experiment_completion_rpc()


#synthetic_worload.g
load_synthetic_stream_graphs('./config/zipf_sw_mdmc_spark_20p.g')


