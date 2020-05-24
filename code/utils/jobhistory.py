#!/usr/bin/python

import requests
import json
import pandas as pd 
import matplotlib.pyplot as plt
import seaborn as sns; 
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import math


sns.set(color_codes=True)

def format_xticks(x, pos=None):
    max_cache_file=256
    total_size = 32
    return str(int(total_size*x/max_cache_file))


def process_tasks(data):
    tasks = get_executed_tasks(data['jobid']);
    
    stats = {
    'max_runtime' : 0,
    'min_runtime' : -1,
    'sum_runtime' : 0,
    'n_maps' : 0,
    'avg_runtime': 0}
    
    for t in tasks:
        if t['type'] == 'MAP':
            stats['n_maps'] += 1
            stats['sum_runtime'] += (t['elapsedTime']/1000)
            if t['elapsedTime'] > stats['max_runtime']:
                stats['max_runtime'] = t['elapsedTime']
            elif t['elapsedTime'] < stats['min_runtime'] or stats['min_runtime'] == -1:
                stats['min_runtime'] = t['elapsedTime']
    data['map_avg'] = round(stats['sum_runtime']/stats['n_maps'], 3)            
    data['map_min'] = round(stats['min_runtime']/1000, 2) if stats['min_runtime']/1000 > 0 else 0
    data['map_max'] = round(stats['max_runtime']/1000, 2)
    
    job_info = get_executed_job(data['jobid']);
    data['runtime'] = (job_info['finishTime'] - job_info['startTime'])/1000
    data['queuetime'] = (job_info['startTime'] - job_info['submitTime'])/1000
    
    return data

def get_executed_job(job_id):
    host_name='neu-5-1'
    port=19888
    url = 'http://%s:%d/ws/v1/history/mapreduce/jobs/%s'%(host_name, port, job_id)
    r=requests.get(url)
    return r.json()['job']

def get_executed_tasks(job_id):
    host_name='neu-5-1'
    port=19888
    url = 'http://%s:%d/ws/v1/history/mapreduce/jobs/%s/tasks'%(host_name, port, job_id)
    r=requests.get(url)
    return r.json()['tasks']['task']

