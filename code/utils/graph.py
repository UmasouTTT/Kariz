#!/usr/bin/python3
# Trevor Nogues, Mania Abdi

# Graph abstraction 
from collections import defaultdict 
import random
import sched, threading, time
import utils.randoms
import uuid
import utils.plan as plan
import ast
import utils.job as jb
import numpy as np
import enum
import copy
import graph_tool.all as gt
import re
from colorama import Fore, Style

# creating enumerations using class 
class Type(enum.IntEnum): 
    tiny = 0
    sequential = 1
    aggregate = 2
    broadcast = 3
    complex = 4

#Class to represent a graph 
class Graph: 
    def __init__(self, n_vertices = 0, type = Type.complex, name='graph'):
        self.dag_id = 0

        self.n_vertices = n_vertices 
        self.jobs = {}
        for i in range(0, n_vertices):
            self.jobs[i] = jb.Job(i)
        
        self.misestimated_jobs = np.zeros(2*n_vertices)
        
        self.roots = set(range(0, n_vertices))
        self.leaves = set(range(0, n_vertices))
        self.blevels = {}

        self.g_gt = None;

        self.mse_factor = 0
        self.plans_container = None
        self.stages = {}
        self.name = name
        self.category = type
        self.submit_time = 0
        self.queue_time = 10 # 10 second from now; should be configurable
        self.total_runtime = 0 

    def reset(self):
        for j in self.jobs:
            job = self.jobs[j]
            job.reset()
        self.stages = {}
        self.plans_container = None
        self.blevels = {}    
    
    def __str__(self):
        graph_str = '{ "jobs": ['
        for j in self.jobs:
            graph_str += str(self.jobs[j])
            graph_str += ','
        graph_str = graph_str[:-1]
        graph_str = graph_str  + '], "uuid": "' + str(self.dag_id) 
        graph_str = graph_str  + '", "n_vertices" : ' + str(self.n_vertices)
        graph_str = graph_str  +  ', "mse_factor" : ' + str(self.mse_factor)
        graph_str = graph_str  +  ', "name" : "' + str(self.name) 
        graph_str = graph_str  +  '", "submit_time" : ' + str(self.submit_time) + ', "queue_time" : ' + str(self.queue_time)
        graph_str = graph_str  +  ', "total_runtime" : ' + str(self.total_runtime) + '}'  
        return graph_str

    def add_new_job(self, value):
        self.jobs[self.n_vertices] = jb.Job(self.n_vertices)
        self.n_vertices+= 1
    
    def set_misestimated_jobs(self, mse_jobs):
        i = 0
        for x in mse_jobs:
            if x == 1:
                i+= 1

        for i in range(0, self.n_vertices):
            self.jobs[i].set_misestimation(mse_jobs[i], mse_jobs[i + self.n_vertices])

        return i, len(mse_jobs)

    def get_misestimated_jobs(self):
        pass

    def config_misestimated_jobs(self): # mse_factor: miss estimation factor
        for i in range(0, self.n_vertices):
            self.jobs[i].config_misestimated_runtimes(self.mse_factor)
    
    def set_misestimation_error(self, mse_factor):
        self.mse_factor = mse_factor;
    
    def config_operation(self, jid, op):
        self.jobs[jid].config_operation(op)
    
    # Randomly assign time value to each node
    def random_runtime(self):
        for i in range(0, self.n_vertices):
            self.jobs[i].random_runtime(1, 10)
            
    def static_runtime(self, v, runtime_remote, runtime_cache):
        self.jobs[v].static_runtime(runtime_remote, runtime_cache)
        
    def config_ntasks(self, v, n_tasks):
        self.jobs[v].config_ntasks(n_tasks)
        
    def config_inputs(self, v, inputs):
        self.jobs[v].config_inputs(inputs)

    def add_edge(self, src, dest, distance = 0):
        if src not in self.jobs:
            self.add_new_job(src)
        if dest not in self.jobs:
            self.add_new_job(dest)
        self.jobs[src].add_child(dest, distance)
        if src in self.leaves:
            self.leaves.remove(src)
            self.jobs[src].blevel = -1
            
        self.jobs[dest].add_parent(src, distance)
        if dest in self.roots:
            self.roots.remove(dest)
            self.jobs[src].tlevel = -1
         
    def bfs(self, s = 0): 
        visited = [False]*(self.n_vertices) 
        bfs_order = []
        queue = list(self.roots)
        for r in self.roots:
            visited[r] = True
            
        while queue: 
            s = queue.pop(0) 
            print (self.jobs[s].id, end = " ")
            bfs_order.append(s) 
  
            for i in self.jobs[s].children: 
                if visited[i] == False: 
                    queue.append(i) 
                    visited[i] = True
    
    def blevel(self):
        if self.blevels:
            return self.blevels
        cur_lvl = 0
        visited = [False]*self.n_vertices
        self.blevels[cur_lvl] = list(self.leaves)
        queue = list(self.leaves)
        for v in self.leaves: 
            visited[v] = True
            queue.extend(self.jobs[v].parents.keys())
        
        while queue:
            s = queue.pop(0)
            if visited[s] : continue
            
            max_children_blvl = -1
            for child in self.jobs[s].children:
                if self.jobs[child].blevel == -1:
                    max_children_blvl = -1
                    queue.append(s)
                    break
                
                if self.jobs[child].blevel > max_children_blvl:
                    max_children_blvl = self.jobs[child].blevel
            if max_children_blvl != -1:
                self.jobs[s].blevel = max_children_blvl + 1
                visited[s] = True
                if self.jobs[s].blevel not in self.blevels: self.blevels[self.jobs[s].blevel] = [] 
                self.blevels[self.jobs[s].blevel].append(s)
                queue.extend(self.jobs[s].parents.keys())
        
        return self.blevels
                
        
    # A recursive function used by topologicalSort 
    def topologicalSortUtil(self,v,visited,stack): 
  
        # Mark the current node as visited. 
        visited[v] = True
  
        # Recur for all the vertices adjacent to this vertex 
        for i in self.graph[v]: 
            if visited[i[0]] == False: 
                self.topologicalSortUtil(i[0],visited,stack) 
  
        # Push current vertex to stack which stores result 
        stack.insert(0,v) 
  
    # The function to do Topological Sort. It uses recursive  
    # topologicalSortUtil() 
    def topologicalSort(self): 
        # Mark all the vertices as not visited 
        visited = [False]*self.V 
        stack =[] 
  
        # Call the recursive helper function to store Topological 
        # Sort starting from all vertices one by one 
        for i in range(self.V): 
            if visited[i] == False: 
                self.topologicalSortUtil(i,visited,stack) 
  
        # Return contents of the stack 
        return stack


        # Helper to update tLevel() contents
    # Same as bLevelHelper()
    def tLevelHelper(self, revGraphCopy, deleted, levels, count):
        checked = [True]*self.V
        for c in range(len(deleted)):
            if deleted[c] == False and revGraphCopy[c] == []:
                checked[c] = False

        for i in range(len(checked)):
            if checked[i] == False:
                deleted[i] = True
                count -= 1
                for node in range(self.V):
                    for subnode in revGraphCopy[node]:
                        if subnode[0] == i:
                            revGraphCopy[node].remove(subnode)

        # print(count, revGraphCopy)
        return count

    # Find t-level of DAG
    def tLevel(self):
        # "Reverse" the graph, then use code for finding b-level
        revGraphCopy = self.revGraph()
        levels = [0]*self.V
        deleted = [False]*self.V
        count = self.V
        while count > 0:
            count = self.tLevelHelper(revGraphCopy,deleted,levels,count)
            for i in range(len(deleted)):
                if deleted[i] == False:
                    levels[i] += 1
        return levels

    def update_runtime(self, plan):
        for j in plan.jobs:
            t_imprv = 0
            for f in plan.data:
                if f in j['job'].inputs:
                    t_imprv_tmp = int(plan.data[f]['size']*(j['job'].runtime_remote - j['job'].runtime_cache)/j['job'].inputs[f])
                    if t_imprv_tmp > t_imprv:
                        t_imprv = t_imprv_tmp
            j['job'].final_runtime = j['job'].runtime_remote - t_imprv #j['improvement']

