#!/usr/bin/python3
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
from workload.generic import Workload
from threading import Thread

import utils.requester as req
import utils.graph as graphs
import utils.jobhistory as hist
import pandas as pd

import graph_tool.all as gt
import os
import ast
import re
import uuid
import json

import d3n.metadata as md
import utils.yarn as yarn
import d3n.d3n_api as api


class Sequential(Workload): 
    def __init__(self, config_file):
        with open(config_file) as fd:
            self.configs = json.load(fd)
            if self.configs['type'] != "sequential":
                raise NameError("Wrong configuration: specifiy sequential as workload type")

            self.graph_skeleton_pool = graphs.load_graph_skeleton(self.configs["graph_skeleton_path"])
            print(json.dumps(self.configs, indent=2))

    
    def select_dags_randomly(self, n_dags):
        return random.choices(list(self.dags.keys()), k = n_dags)
    
    def submit_dag(self, dag_name):
        dag = self.dags[dag_name].copy()
        dag.gp.id = str(uuid.uuid1())
        pigsim.start_pig_simulator(dag)

        self.pendings.remove(threading.current_thread())
        
    def select_and_submit(self, n_dags):
        
        dags = self.select_dags_randomly(n_dags)
        
        for dag_name in dags:
            t = Thread(target=self.submit_dag, args=(dag_name, ))
            t.start()
            self.pendings.append(t)
        
    
    def run(self):
        elapsed_time = 0;
         
        # initialize a timer that issues submit DAG every two seconds
        ticker = threading.Event()
        while elapsed_time < cfg.simulation_period:
            ticker.wait(cfg.submission_interval)
            
            self.select_and_submit(cfg.n_dags_per_interval)
               
            elapsed_time += cfg.submission_interval;

            print(Fore.LIGHTRED_EX, 'Number of pending tasks', len(self.pendings), Style.RESET_ALL)

        # join all DAGs to finish
        for t in self.pendings:
            t.join()

        req.send_experiment_completion_rpc()
        
