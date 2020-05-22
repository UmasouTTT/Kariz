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

    print(Fore.GREEN, 'Execute stage ', g.gp.cur_stage, 'for DAG ', g.gp.name,
          ', jobs:', ready, Style.RESET_ALL)

    req.send_stage_start_rpc(req.serialize_stage(g))
    processes = []
    for v in ready:
        program = 'runner.py'
        inputdir = g.vp['inputdir'][v]
        cache_runtime = g.vp['cache_runtime'][v]
        remote_runtime = g.vp['remote_runtime'][v]
        executable = '%s/%s'%(os.getcwd(),program)
        processes.append(subprocess.Popen([executable, str(inputdir), str(remote_runtime), str(cache_runtime)]))

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
    stages = []
    g.vp['stage_id'] = g.new_vertex_property("int")

    while True:
        to_be_executed = schedule(g)
        if not len(to_be_executed):
            break
        for v in to_be_executed:
            g.vp['status'][v] = 1
            g.vp['stage_id'][v] = len(stages)
        stages.append(to_be_executed)
    
    for v in g.vertices():
        g.vp['status'][v] = 0

    g.gp['stages'] = g.new_graph_property("string", str(stages))


if __name__ == '__main__':
    with open('graphs.g', 'r') as fd:
        graph_strs = fd.read().split('#')[1:]
        for g_str in graph_strs:
            g = build_dag_from_str(g_str)
            execute_dag(g)
            break;

