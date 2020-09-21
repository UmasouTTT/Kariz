#!/usr/bin/python3
import sys
import ast
import time
import json
import utils.requester as req
import utils.plan as plan
from colorama import Fore, Style

inputdir = ast.literal_eval(sys.argv[1])
cached_size = 0
total_size = 0;
for _input in inputdir:
    inputdir[_input] = {'size': inputdir[_input]}

    cp = plan.Plan()
    cp.data = {_input: inputdir[_input]}
    cache_res = req.is_plan_cached(cp)

    if cache_res > 0:
        cached_size += int(cache_res)

    total_size += inputdir[_input]['size'];

remote_runtime = float(sys.argv[2])

cache_runtime = float(sys.argv[3])

dag_name = sys.argv[4]
stage_name = sys.argv[5]
job_name = sys.argv[6]

# Check if data is in the cache or how much data is in the cache
# Compute the runtime
runtime_reduction = cached_size*(remote_runtime - cache_runtime)//total_size if total_size else 0
execution_time = remote_runtime - runtime_reduction

if execution_time >= 0:
    sleep_time = execution_time
elif execution_time//10 < 10:
    sleep_time = 10
else:
    sleep_time = execution_time//10

if total_size:
    print(Fore.LIGHTYELLOW_EX, "DAG", dag_name, "for stage", stage_name, "Cached data size:", cached_size, "Cache runtime:", cache_runtime,
            "Total data size:", total_size, "Remote runtime:", remote_runtime,
            "execution time:", execution_time, Fore.LIGHTBLUE_EX, "Sleep for ", sleep_time, Style.RESET_ALL)
else:
    print(Fore.WHITE, "DAG", dag_name, "for stage", stage_name, "Cached data size:", cached_size, "Cache runtime:", cache_runtime,
            "Total data size:", total_size, "Remote runtime:", remote_runtime,
            "execution time:", execution_time, Fore.LIGHTBLUE_EX, "Sleep for ", sleep_time, Style.RESET_ALL)

print(json.dumps({'cached_size': cached_size, 'total_size': total_size,
            'runtime': execution_time, 'improvement': remote_runtime - execution_time,
            'remote_runtime': remote_runtime, 'cache_time': cache_runtime, 'timestamp': time.time(),
            'dag_id': dag_name, 'stage_id': stage_name, 'job_id': job_name}),
      file=sys.stderr)


# Sleep for the execution time
time.sleep(sleep_time)
