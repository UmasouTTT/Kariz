#!/usr/bin/python3

from random import randrange
from time import sleep
import threadpool as tp


delays = [randrange(1, 10) for i in range(30)]

def prefetch_data(bucket):
    print('sleeping for (%d)sec' % d)
    sleep(d)

pool = tp.ThreadPool(20)
for i, d in enumerate(delays):
    pool.add_task(wait_delay, d)
pool.wait_completion()
