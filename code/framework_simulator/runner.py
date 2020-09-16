#!/usr/bin/python3
import sys
import ast
import time
import utils.requester as req
import utils.plan as plan

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


# Check if data is in the cache or how much data is in the cache
# Compute the runtime
runtime_reduction = cached_size*(remote_runtime - cache_runtime)//total_size if total_size else 0
execution_time = remote_runtime - runtime_reduction
print("Cached data size:", cached_size, "Cache runtime:", cache_runtime,
      "Total data size:", total_size, "Remote runtime:", remote_runtime,
      "execution time:", execution_time)


# Sleep for the execution time
print("Sleep for ", execution_time)
time.sleep(execution_time)
