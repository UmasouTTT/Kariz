#!/usr/bin/python3
'''
Created on Sep 16, 2019

@author: mania
'''
import gateway
import datetime
import json
from colorama import Fore, Style


#workload.load_graphs('config/synthetic_dags_2_1_sharing.g')
#workload.start_experiment()
def load_synthetic_stream_graphs(fpath):
    runtime_stats = {}
    with open(fpath, 'r') as fd:
        wstr = fd.read()
        workload = gateway.Workload()
        start_time = datetime.datetime.now()
        workload.load_graphs_fromstring(wstr)
        runtime_stats = workload.start_experiment()
        runtime_stats['exec_time'] = (datetime.datetime.now() - start_time).total_seconds()
    print(Fore.RED, 'End-to-end experiment runtime %d'%((datetime.datetime.now() - start_time).total_seconds()), Style.RESET_ALL)
    with open('md_sw_zipf_pig_100p_cmr.json', 'w') as fd:
        fd.write(json.dumps(runtime_stats))


#synthetic_worload.g
load_synthetic_stream_graphs('./config/zipf_synthetic_worload_md_100p_pig.g')


