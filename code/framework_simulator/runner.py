#!/usr/bin/python3
import sys
import ast
import time
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

# Check if data is in the cache or how much data is in the cache
# Compute the runtime
runtime_reduction = cached_size*(remote_runtime - cache_runtime)//total_size if total_size else 0
execution_time = remote_runtime - runtime_reduction

if total_size:
    print(Fore.LIGHTYELLOW_EX, "DAG", dag_name, "for stage", stage_name, "Cached data size:", cached_size, "Cache runtime:", cache_runtime,
            "Total data size:", total_size, "Remote runtime:", remote_runtime,
            "execution time:", execution_time, Fore.LIGHTBLUE_EX, "Sleep for ", execution_time, Style.RESET_ALL)
else:
    print(Fore.WHITE, "DAG", dag_name, "for stage", stage_name, "Cached data size:", cached_size, "Cache runtime:", cache_runtime,
            "Total data size:", total_size, "Remote runtime:", remote_runtime,
            "execution time:", execution_time, Fore.LIGHTBLUE_EX, "Sleep for ", execution_time, Style.RESET_ALL)


# Sleep for the execution time
time.sleep(execution_time)
