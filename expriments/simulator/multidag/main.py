#!/usr/bin/python
'''
Created on Sep 16, 2019

@author: mania
'''
import gateway


#workload.load_graphs('config/synthetic_dags_2_1_sharing.g')
#workload.start_experiment()

def load_synthetic_stream_graphs(fpath):
    with open(fpath, 'r') as fd:
        workload_strs = fd.read().split('%')[1:]
        for wstr in workload_strs:
            workload.load_graphs_fromstring(wstr)
            workload.start_experiment()
            break


#synthetic_worload.g
workload = gateway.Workload()
load_synthetic_stream_graphs('./config/synthetic_worload.g')
