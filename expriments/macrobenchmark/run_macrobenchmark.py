#!/usr/bin/python3

# this is a benchmark to execute a DAG

# TODO: Run time two times one with 256M block size and once with 512 MB
# FIXME: This run: 256MB block size


import sys
import cache 
import swift 
import config as cfg
import os
import subprocess

def run():
   metadata = swift.load_metadata();
   token = swift.get_token()

   tpch_src = "/local0/Kariz/expriments/benchmark/spark/tpch-spark"
   curdir = os.getcwd()
 
   for rep in range(0, cfg.reps):
      n_experiment = len(cfg.experiment_name)
      for idx in range(0, n_experiment):
          cache.clear_cache(token=token);
          print("Start experiment --->  ", cfg.experiment_name[idx], ", rep", rep)
          prefetch_ds = dict(zip(cfg.dpath[idx].split(','), cfg.strides[idx].split(',')))
          for pre_ds in prefetch_ds:
              stride = int(prefetch_ds[pre_ds])
              print('Cache warm up --> Prefetch %d strides from %s'%(stride, pre_ds))
              if stride != 0:
                  cache.prefetch_dataset_stride(metadata, token, pre_ds, stride=stride)

          os.chdir(tpch_src)
          print(os.getcwd())
          proc = subprocess.Popen(["spark-submit", '--class', 'main.scala.TpchQuery', "--master", "spark://neu-3-1:7077", 
              "target/scala-2.11/spark-tpc-h-queries_2.11-1.0.jar" , "15", "s3a://data/pig-tpch/64G/"]) 
          ret = proc.wait()
          os.chdir(curdir)


run()
