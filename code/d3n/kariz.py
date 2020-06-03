#!/usr/bin/python3
# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4

import queue
from io import StringIO
import csv
import threading
import os
import ast
import mirab as mq
import framework_simulator.tpc as tpc

import utils.graph as graphs
import utils.jobhistory as hist
import pandas as pd

import graph_tool.all as gt
import re
import uuid
import json

import d3n.metadata as md
import utils.yarn as yarn
import d3n.d3n_api as api


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

    def __init__(self,config_file):
        global _kariz

        #graph.load_graph_pools(tpc.load_synthetic_graphs(), tpc.build_tpc_graphpool())

        #self.load_graphs_skeleton(config_file)


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
        print(dag_string)
        #self.gq.put(dag_string)

    def notify_new_stage_from_string(self, stage_metastr):
        print(stage_metastr)
        #self.pq.put(stage_metastr)
        
    def remove_dag(self, dagstr):
        self.dq.put(dagstr)

    def end_of_experiment_alert(self):
        self.mirab.end_of_experiment_alert()


    def load_graphs_skeleton(self, config_file):
        with open(config_file) as fd:
            self.configs = json.load(fd)
            if self.configs['type'] != "sequential":
                raise NameError("Wrong configuration: specifiy sequential as workload type")

            self.graphs_pool = {}
            self.graph_skeleton_pool = graphs.load_graph_skeleton(self.configs["graph_skeleton_path"])
            print(json.dumps(self.configs, indent=2))

            self.jobs_stats = pd.read_csv(self.configs['stat_file_path'])
            #self.jobs_stats = self.jobs_stats[self.jobs_stats['runtime'] == 0].apply(hist.process_tasks, axis=1)
            #self.jobs_stats.to_csv(self.configs['stat_file_path'], index=False, header=True)

            self.metadata = md.load_metadata(self.configs['rgw_host'], self.configs['rgw_port'],
                        self.configs['swift_user'], self.configs['swift_key'],
                        self.configs['bucket_name'])

            self.token = md.get_token(self.configs['rgw_host'], self.configs['rgw_port'],
                    self.configs['swift_user'], self.configs['swift_key'])


            input_dir = self.configs['input_dir'].replace('s3a://data/', '')

            self.ds_meta = api.get_dataset_metadata(self.metadata, input_dir)
            
            for g_name in self.graph_skeleton_pool:
                g = self.graph_skeleton_pool[g_name]
                
                for v in g.vertices():
                    remote_runtime = self.jobs_stats[(self.jobs_stats['query'] == g_name) & (self.jobs_stats['node_id'] == int(v)) & (self.jobs_stats['type'] == 'remote')]['runtime'].values[0]
                    cache_runtime = self.jobs_stats[(self.jobs_stats['query'] == g_name) & (self.jobs_stats['node_id'] == int(v)) & (self.jobs_stats['type'] == 'cache')]['runtime'].values[0]
                    
                    g.vp.cache_runtime[v] = cache_runtime
                    g.vp.remote_runtime[v] = remote_runtime
                    
                    for f in g.vp.inputs[v]:
                        if f in self.ds_meta:
                            g.vp.inputs[v][f] = self.ds_meta[f]['size']

                self.graphs_pool[g_name] = graphs.build_graph_from_gt(g)

