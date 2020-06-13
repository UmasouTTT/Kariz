#!/usr/bin/python3

# this is a benchmark to execute a DAG

# TODO: Run time two times one with 256M block size and once with 512 MB
# FIXME: This run: 256MB block size

import json
import sys
import os
import subprocess

def run(conf_file):
   curdir = os.getcwd()

   with open(conf_file, 'r') as fd:
      confs = json.load(fd)
      parallel_queries = confs['n_concurrent_query'] 
      query = confs['query'] 
      reps = confs['repeats']
      for rep in range(0, reps):
         process = []
         for i in range(0, parallel_queries):
            inputdir =  confs['input_dir']
            outputdir = confs['output_dir'] + '-%d'%(i)
            query = confs['query']
            n_reducers = confs['n_reducer']
            executable = confs['benchmark_path'] + '/run_tpch_query.sh'
            mapsize = confs['mapsize']
            curdir = os.getcwd()
            os.chdir(confs['benchmark_path'])
            print(os.getcwd())
            app_name = 'pquery-%d,rep-%d,query-%d,cid-%d'%(parallel_queries, rep, query,i)
   
            process.append(subprocess.Popen([executable, inputdir, outputdir, str(n_reducers), str(query), str(mapsize), app_name]))

         jobs_status = [p.wait() for p in process]
         os.chdir(curdir)

if __name__ == "__main__":
    config_file = sys.argv[1]
    run(config_file)
