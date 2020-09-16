#!/usr/bin/python3
import common
import random
import configs.tpchall as cfg
import d3n.metadata as md
import os
import subprocess
import time
from colorama import Fore, Style

def run_single_experiment(dataset, stride, app_name, experiment_metadata):
    mapsize = 512*1024*1024

    obj_store.clear_cache();
    prefetch_results = []
    if stride:
        for tbl in cfg.tables:
            path, requested, prefetched = obj_store.prefetch_s3_dataset('%s/%s'%(dataset, tbl), cfg.prefered_map_size, stride);
            prefetch_results.append({'app_name': app_name, 'path': path, 'stride': stride, 'requested': requested, 'prefetched': prefetched})

    print(Fore.LIGHTYELLOW_EX, '\tStart query %s execution'%(experiment_metadata['query']), Style.RESET_ALL)
    print(app_name)
    cur_dir = os.getcwd()
    os.chdir(cfg.benchmark_root)
    process = subprocess.Popen([cfg.executable, dataset, 
        cfg.output_path, '1', str(experiment_metadata['query']), str(mapsize)], 
        stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdoutdata, stderrdata = process.communicate()
   
    os.chdir(cur_dir)
    time.sleep(5)
    common.update_statistics(cfg.framework, app_name, cfg.statfile, stdoutdata, prefetch_results)


def get_all_experiments():
    possible_experiemnts = {}
    for bw in cfg.rgw_rates:
        for st in cfg.strides:
            exp_name = '%s:%d'%(bw, st)
            possible_experiemnts[exp_name] = {'bandwidth': bw, 'dataset': cfg.input_path, 'stride': st}
    return possible_experiemnts
    
def run_runtime_prediction_benchmark():
    queries = [f.replace('Q', '').replace('.pig', '') for f in os.listdir('%s/queries'%(cfg.benchmark_root)) if f.endswith('.pig')]
    random.shuffle(queries)
    possible_experiemnts = get_all_experiments()

    for ds in cfg.datasets:
        for q in queries:
            exp_name = random.choice(list(possible_experiemnts))
            experiment = possible_experiemnts[exp_name]
            experiment['query'] = q
            experiment['dataset'] = '%s/%s'%(cfg.input_path, ds)
            app_name='framework:%s:name:%s%s-bw:%s-ds:%s-stride:%d'%(cfg.framework, cfg.app_name, experiment['query'],
            experiment['bandwidth'], experiment['dataset'].split('/')[-1], experiment['stride'])
            print(Fore.LIGHTGREEN_EX, '\tRun %s for stride %d'%(app_name, experiment['stride']), Style.RESET_ALL)
            common.configure_ceph_bw(cfg.config_bw_playbook, cfg.rgw_nic, experiment['bandwidth'])
            common.restart_rgw(cfg.restart_rgw_playbook)
            run_single_experiment(experiment['dataset'], experiment['stride'], app_name, experiment)

obj_store = md.ObjectStore();
obj_store.load_metadata();
run_runtime_prediction_benchmark()
