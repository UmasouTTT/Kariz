#!/usr/bin/python3
import config as cfg 
import math
import cache
import swift
import json 
import os
import sys
import time
import platform
import yarn
import subprocess



metadata = swift.load_metadata();
#print(json.dumps(metadata, indent=4))

token = swift.get_token()
cache.clear_cache(token=token);

strides = [0, 4, 8, 16, 24, 32, 48, 64, 80, 96, 112, 128, 
        144, 160, 176, 192, 208, 224, 240, 256]


report_file = open(cfg.report_file, 'a')

for stride in strides:
    for rep in range(0, cfg.reps):
        print('Start microbenchmark for stride %d, reps %d'%(stride, rep))

        cache.clear_cache(token=token);

        if stride:
            cache.prefetch_dataset_stride(metadata, token, cfg.dpath, stride=stride)

        # cd to HiBench directory
        cur_dir = os.getcwd()
        os.chdir(cfg.hibench_root)

        print('--------------------------------------------------->>>>>>>>>>>>>>>>>>>>>>.', os.getcwd())
        # run benchmark subprocess
        #app = asyncio.run(run(cfg.hive_command));
        process = subprocess.Popen([cfg.hibench_command], stdout=subprocess.PIPE, shell=True, executable="/bin/bash")

        print('Wait for ', cfg.grace_time)
        time.sleep(cfg.grace_time)

        # get app name
        app = yarn.get_appname();


        process.wait()



        # get statitics
        #yarn.process_job()

        # cd to the current directory
        os.chdir(cur_dir)
        
        # write to report. 
        report_file.write('%s,%s,%d,%d\n'%(app, cfg.app_name, stride, rep))

        print('\n\n\n')
#        break;
#    break;

report_file.close()
