#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import queue
from io import StringIO
import csv
import threading
import os
import ast
import utils.graph as graph
import mirab as mq
import roundrobin as rr
import sjf
import framework_simulator.tpc as tpc

from colorama import Fore, Style

_kariz = None

class Kariz:
    def gq_worker(self):
        while True:
            graphstr = self.gq.get(block=True, timeout=None)
            if graphstr:
                g = graph.str_to_graph(graphstr, self.objectstore)
                self.mirab.add_dag(g)
                self.gq.task_done()


    def pq_worker(self):
        while True:
            stage_metastr = self.pq.get()
            if stage_metastr:
                stage_meta = ast.literal_eval(stage_metastr)
                self.mirab.online_planner(stage_meta['id'], stage_meta['cur_stage'])
                self.pq.task_done()
                
    def dq_worker(self):
        while True:
            dagstr = self.dq.get()
            if dagstr:
                dag_meta = ast.literal_eval(dagstr) 
                self.mirab.delete_dag(dag_meta['id'])
                self.dq.task_done()

    def __init__(self):
        global _kariz

        graph.load_graph_pools(tpc.load_synthetic_graphs(), tpc.build_tpc_graphpool())


        # a thread to process the incoming dags
        self.gq = queue.Queue();
        self.gt = threading.Thread(target=self.gq_worker)
        self.gt.start()
        # a thread to process the incoming stage 
        self.pq = queue.Queue();
        self.pt = threading.Thread(target=self.pq_worker)
        self.pt.start()
        
        self.dq = queue.Queue();
        self.dt = threading.Thread(target=self.dq_worker)
        self.dt.start()
        
        self.objectstore = None
        self.mirab = mq.Mirab() # Mirab logic

        #self.mirab = rr.RoundRobin(bandwidth=30) 
        _kariz = self # mirab daemon instance 

    def new_dag_from_string(self, dag_string):
        self.gq.put(dag_string)

    def notify_new_stage_from_string(self, stage_metastr):
        self.pq.put(stage_metastr)
        
    def remove_dag(self, dagstr):
        self.dq.put(dagstr)

    def end_of_experiment_alert(self):
        self.mirab.end_of_experiment_alert()

