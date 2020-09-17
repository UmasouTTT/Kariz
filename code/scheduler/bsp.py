#!/usr/bin/python3

import graph_tool.all as gt
import os
#import scheduler.config as cfg
import time
import subprocess
import utils.requester as req
from colorama import Fore, Style

'''
Bulk Synchroneous parallel scheduling which is followed by spark PIG and hive 
'''

def submit_and_execute_stage(g, ready=[]):
    if not len(ready): return;
    g.gp.cur_stage = g.vp.stage_id[ready[0]]

    print(Fore.GREEN, 'Execute stage ', g.gp.cur_stage, 'for DAG ', g.gp.id,
          ', jobs:', ready, Style.RESET_ALL)

    req.send_stage_start_rpc(req.serialize_stage(g))
    processes = []
    for v in ready:
        frameworksim = '/home/mania/Northeastern/MoC/Kariz/code/framework_simulator'
        program = 'runner.py'
        g.vp.job[v].predict_runtime(10, 100)
        inputdir = g.vp.job[v].inputs
        cache_runtime = g.vp.job[v].runtime_cache
        remote_runtime = g.vp.job[v].runtime_remote
        executable = '%s/%s'%(frameworksim,program)
        processes.append(subprocess.Popen([executable, str(inputdir), str(remote_runtime), str(cache_runtime), str(g.gp.id), str(g.gp.cur_stage)]))

    jobs_status = dict(zip(ready, [p.wait()  for p in processes]))
    
    for v in jobs_status:
        g.vp['status'][v] = 1 if not jobs_status[v] else jobs_status[v]


def schedule(g):
    sort = gt.topological_sort(g)
    schedule = 1;
    to_be_executed = []
    for v in sort:
        if g.vp.status[v] == 1: # already executed
            continue;
        schedule = 1; 
        
        # get in-degree neighbors
        in_neight_vrtx = g.get_in_neighbors(v)
        for nv in in_neight_vrtx:
            if g.vp.status[nv] == 0:
                schedule = 0;
        if schedule:
            to_be_executed.append(v) 
    return to_be_executed

def execute_dag(g):
    while True:
        to_be_executed = schedule(g)
        if not len(to_be_executed):
            break
        submit_and_execute_stage(g, to_be_executed)


def assign_stages(g):
    stages = {}
    stage_id = 0
    while True:
        to_be_executed = schedule(g)
        if not len(to_be_executed):
            break
        for v in to_be_executed:
            g.vp['status'][v] = 1
            g.vp['stage_id'][v] = stage_id
        stage_id+=1
    for v in g.vertices():
        g.vp['status'][v] = 0


if __name__ == '__main__':
    with open('graphs.g', 'r') as fd:
        graph_strs = fd.read().split('#')[1:]
        for g_str in graph_strs:
            g = build_dag_from_str(g_str)
            execute_dag(g)
            break;

