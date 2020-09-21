#!/usr/bin/python
'''
Created on Sep 7, 2019

@author: mania
'''
import json

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
from prwlock import RWLock

class Workload: 
    def __init__(self):
        self.n_intervals = 0
        self.dags = {}
        self.pendings = []
        self.object_store = objs.load_object_meta('./config/inputs.csv')
        self.pendings_mutex = RWLock()
        self.dags_stats = {}
        self.next_start = 0
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
        dag.gp.start_time = self.next_start
        print('submit %d'%(dag_name))
        return pigsim.start_pig_simulator(dag)


    def run(self, dag_name):
        start_time = datetime.datetime.now()
        stats, runtime, finish_time = self.submit_dag(dag_name)
        with self.pendings_mutex.writer_lock():
            self.pendings.remove(threading.current_thread())
            self.dags_stats[dag_name] = {
                'stats': stats, 'name': dag_name, 'runtime': runtime}
            if finish_time > self.next_start:
                self.next_start = finish_time
        print(Fore.LIGHTRED_EX, 'DAG', dag_name, ' was finished in', runtime, Style.RESET_ALL)
        return

    def start_experiment(self):
        elapsed_time = 0;
        req.clear_cache()
        # initialize a timer that issues submit DAG every two seconds
        self.pendings = []
        pendings = []
        self.dags_stats = {}
        start_times = {}
        start_time = datetime.datetime.now()  
        for _id, gid in enumerate(self.dags):
            t = Thread(target=self.run, args=(gid,))
            with self.pendings_mutex.writer_lock():
                self.pendings.append(t)
                start_times[gid] = self.next_start
            pendings.append(t)
            t.start()

            print(Fore.LIGHTRED_EX, 'Number of pending DAGs', len(self.pendings), Style.RESET_ALL)
            while True:
                with self.pendings_mutex.reader_lock():
                    if len(self.pendings) < 10:
                        print('break')
                        break
                


        for t in pendings:
            t.join()
        
        print(Fore.YELLOW, 'Experiment was running for %d'%((datetime.datetime.now() - start_time).total_seconds()), 
                Fore.LIGHTRED_EX, 'Number of pending DAGs', len(self.pendings), Style.RESET_ALL)

        results = {'statistics': self.dags_stats,
                'start_time': start_times}
        return results
