#!/usr/bin/python3
import utils
import configs.wc as cfg
import d3n.metadata as md
import os
import subprocess
import time
from colorama import Fore, Style

def run_single_experiment(dataset, stride, app_name):
    obj_store.clear_cache();
    print('Cache is cleared, check the tmp directory')

    if stride:
        print('Strideeeeeeee', stride)
        obj_store.prefetch_s3_dataset(dataset, cfg.prefered_map_size, stride);

    print('Prefetched the requested strides, check the tmp directory')
    time.sleep(30)

    
    cur_dir = os.getcwd()
    os.chdir(cfg.benchmark_root)
    process = subprocess.Popen([cfg.executable, dataset, cfg.output_path, app_name], stderr=subprocess.PIPE)
    stdoutdata, stderrdata = process.communicate()
    
    #process = subprocess.Popen([cfg.executable, dataset, cfg.output_path, app_name])
    #process.wait()

    os.chdir(cur_dir)
    time.sleep(5)
    #utils.update_statistics(cfg.framework, app_name, cfg.statfile)


def run_wrepeats_all_strides(dataset, app_name=''):  
    for rep in range(0, cfg.repeats):
        for stride in cfg.strides:
            print(Fore.LIGHTGREEN_EX, '\tRun %s for stride %d, reps %d'%(app_name, stride, rep), Style.RESET_ALL)
            #app_name2='%s-stride:%d-rep:%d-%s'%(app_name,stride,rep,utils.randomString())
            app_name2 = '%s-stride:%d-rep:%d' % (app_name, stride, rep)
            run_single_experiment(dataset, stride,app_name2)
        break


def run_runtime_prediction_benchmark():
    for bandwidth in cfg.rgw_rates:
#        utils.configure_ceph_bw(cfg.playbook, cfg.rgw_nic, bandwidth)

        for ds in cfg.datasets:
            print(Fore.LIGHTGREEN_EX, 'Run %s for bandwidth %s, dataset %s'%(cfg.app_name, bandwidth, ds), Style.RESET_ALL)
            app_name='name:%s-bw:%s-ds:%s'%(cfg.app_name, bandwidth, ds)
            run_wrepeats_all_strides(cfg.input_path+ds, app_name=app_name)
            break
        utils.clear_spark_tmp_directory(cfg.clear_spark_playbook)
        break


obj_store = md.ObjectStore();
obj_store.load_metadata();
run_runtime_prediction_benchmark()
