#!/usr/bin/python
'''
Created on Sep 16, 2019

@author: mania
'''

import workloadgenerator as wkl

def test_misestimation():
    workload = wkl.Workload()
    workload.start_single_dag_coldcache_misestimation()


def test_sequential():
    workload = wkl.Workload()
    workload.start_single_dag_coldcache_seqworkload()

def test_concurrent():
    workload = wkl.Workload()
    workload.start_multiple_dags_workload()
 
#test_sequential()
#test_concurrent()
test_misestimation()