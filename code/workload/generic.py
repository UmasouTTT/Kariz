#!/usr/bin/python
'''
Created on Sep 7, 2019

@author: mania
'''

'''
from alibaba traces we figure it out that average # of DAGs
submitted per 30 is L. The maxumum is Y and the minimum is Z

we generate DAGs for 1 hour every 30 seconds using poisson distribution 
with average time interval of L

For alibaba traces should I randomly 
lets independently apply size and identity 

generate set of N filenames 
Usibg exponential propablity to assign sizes for filenames use zipf distribution 
I use zipf to select from a list of file names

for ali baba traces just randomly assign inputs to nodes. 


'''
import random 
import threading
from colorama import Fore, Style
from threading import Thread
import uuid
import utils.requester as req


class Workload: 
    def __init__(self):
        pass

    def select_dags_randomly(self, n_dags):
        return random.choices(list(self.dags.keys()), k = n_dags)
    
    def submit_dag(self, dag_name):
        pass
        
    def select_and_submit(self, n_dags):
        
        dags = self.select_dags_randomly(n_dags)
        
        for dag_name in dags:
            t = Thread(target=self.submit_dag, args=(dag_name, ))
            t.start()
            self.pendings.append(t)
        
    
    def run(self):
        req.send_experiment_completion_rpc()
