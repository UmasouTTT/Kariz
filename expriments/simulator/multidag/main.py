#!/usr/bin/python
'''
Created on Sep 16, 2019

@author: mania
'''
import gateway

workload = gateway.Workload()
workload.load_graphs('config/synthetic_dags_2_1_sharing.g')
workload.start_experiment()
