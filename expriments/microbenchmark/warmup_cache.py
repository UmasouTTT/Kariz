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

stride = int(sys.argv[1])
dpath = os.getenv('dpath')

print('Cache warm up --> Clear Start microbenchmark for stride %d'%(stride))
if stride:
   cache.prefetch_dataset_stride(metadata, token, dpath, stride=stride)
