#!/usr/bin/python3


import requests as req
import json
import pandas as pd

port = 19888
hostname='neu-17-6'
uri = f'http://{hostname}:{port}/ws/v1/history/mapreduce/jobs' 
r = req.get(uri)
jobs = r.json()['jobs']['job']

metadata = []

for job in jobs:
   jobid = job['id']
   jobname = job['name']
   input_bytes = 1
   output_bytes = 0
   r = req.get(f'{uri}/{jobid}/counters')
   counters = r.json()["jobCounters"]["counterGroup"]
   for cgroup in counters:
      if 'org.apache.hadoop.mapreduce.FileSystemCounter' in cgroup['counterGroupName']:
          io_counters = cgroup["counter"]
          for iocnt in io_counters:
             if 'HDFS_BYTES_READ' in iocnt['name']: 
                input_bytes= iocnt['totalCounterValue']
             if 'HDFS_BYTES_WRITTEN' in iocnt['name']:
                output_bytes= iocnt['totalCounterValue']
   #print(job['id'], job['name'], input_bytes, output_bytes, f'{round(100*output_bytes/input_bytes, 3)}%')
   metadata.append({'jobid': job['id'], 'jobname': job['name'], 'inputs': input_bytes, 'outputs': output_bytes, 'selectivity': 100*output_bytes/input_bytes})

df = pd.DataFrame(data=metadata)

print(df['selectivity'].describe(), df['selectivity'].max(), df['selectivity'].min(), df['selectivity'].mean())
