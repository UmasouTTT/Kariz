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
import framework_simulator.pigsimulator as pigsim
import threading
from colorama import Fore, Style
from threading import Thread
import uuid
import utils.requester as req
import utils.gbuilders as gbuilder
import utils.objectstore as objs
import datetime

class Workload: 
    def __init__(self):
        self.n_intervals = 0
        self.dags = {}
        self.pendings = []
        self.object_store = objs.load_object_meta('./config/inputs.csv')
        pass

    def load_graphs(self, fpath):
        self.dags = gbuilder.load_synthetic_dags(fpath, self.object_store)
        pass

    def load_graphs_fromstring(self, g_str):
        self.dags = gbuilder.load_tpc_dags_from_string(g_str, self.object_store)
        pass

    def select_dags_randomly(self, n_dags):
        return random.choices(list(self.dags.keys()), k = n_dags)
    
    def submit_dag(self, dag_name):
        dag = self.dags[dag_name].copy()
        dag.gp.uuid = str(uuid.uuid1())
        print('submit %d'%(dag_name))
        pigsim.start_pig_simulator(dag)
        pass


    def run(self, dag_name):
        start_time = datetime.datetime.now()
        self.submit_dag(dag_name)
        self.pendings.remove(threading.current_thread())
        print(Fore.LIGHTRED_EX, 'DAG', dag_name, ' was finished in', (datetime.datetime.now() - start_time).total_seconds(), Style.RESET_ALL)
        return

    def start_experiment(self):
        elapsed_time = 0;
        # initialize a timer that issues submit DAG every two seconds
        start_time = datetime.datetime.now()  
        for gid in self.dags:
            t = Thread(target=self.run, args=(gid,))
            self.pendings.append(t)
            t.start()

        print(Fore.LIGHTRED_EX, 'Number of pending DAGs', len(self.pendings), Style.RESET_ALL)
        for t in self.pendings:
            t.join()
        
        print(Fore.YELLOW, 'Experiment was running for %d'%((datetime.datetime.now() - start_time).total_seconds()), 
                Fore.LIGHTRED_EX, 'Number of pending DAGs', len(self.pendings), Style.RESET_ALL)
        #req.send_experiment_completion_rpc()

