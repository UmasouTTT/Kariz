#!/usr/bin/python3

# this is a benchmark to execute a DAG

# TODO: Run time two times one with 256M block size and once with 512 MB
# FIXME: This run: 256MB block size


import scheduler
import sys
import graph_tool.all as gt
import time
import cache 
import swift 
import aonvpconf as cfg

def build_dag_from_str(g_str):
    g_elements=g_str.split('\n')[:-1]
    g = gt.Graph(directed=True)
    status = g.new_vertex_property("int")
    ops = g.new_vertex_property("string")
    inputs = g.new_vertex_property("string")
    outputs = g.new_vertex_property("string")
    mapsizes = g.new_vertex_property("int")

    for g_e in g_elements:
        command, params = g_e.split(',', 1)

        if command == 'v':
            vid,operation, inputdir, outdir, mapsize = params.split(',')
            v = g.add_vertex()
            status[v] = 0
            ops[v] = operation
            inputs[v] = inputdir
            outputs[v] = outdir
            mapsizes[v] = int(mapsize)
        elif command == 'e':
            src, dest = params.split(',')
            g.add_edge(src, dest)
    g.vp['status'] = status;
    g.vp['ops'] = ops;
    g.vp['inputdir'] = inputs
    g.vp['outputdir'] = outputs
    g.vp['mapsize'] = mapsizes
    return g;


def emulate_pig_execution(g):
    scheduler.execute_dag(g)


def run():
   metadata = swift.load_metadata();
   token = swift.get_token()
  
   input_file = sys.argv[1]
   with open(input_file, 'r') as fd, open(cfg.report_file, 'a') as fdw:
       graph_strs = fd.read().split('#')[1:]
    
       for g_str in graph_strs: # for each graph 
           for rep in range(0, cfg.reps):
              n_experiment = len(cfg.dpath)
              for idx in range(0, n_experiment):
                  cache.clear_cache(token=token);
                  prefetch_ds = dict(zip(cfg.dpath[idx].split(','), cfg.strides[idx].split(',')))
                  for pre_ds in prefetch_ds:
                      stride = int(prefetch_ds[pre_ds])
                      print('Cache warm up --> Prefetch %d strides from %s'%(stride, pre_ds))
                      if stride:
                          cache.prefetch_dataset_stride(metadata, token, pre_ds, stride=stride)
      
                  g = build_dag_from_str(g_str)
                  start_ts = time.time()
                  emulate_pig_execution(g)
                  end_ts = time.time()
                  runtime = end_ts - start_ts
                  fdw.write('%s,%s,512,%d,%d\n'%(cfg.app_name,cfg.experiment_name[idx],rep,int(runtime)))
                  print('------>>>> DAG execution time:', runtime)


run()
