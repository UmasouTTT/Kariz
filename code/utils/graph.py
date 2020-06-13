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

def str_to_graph(raw_execplan, objectstore):
    g = None

    if raw_execplan.startswith('DAG'):
        g = pigstr_to_graph(raw_execplan, objectstore)
    elif raw_execplan.startswith('ID'):
        g = graph_id_to_graph_id(raw_execplan, objectstore)
    else:
        gjson = ast.literal_eval(raw_execplan)
        if gjson['type'] == 'synthetic':
            g = graph_id_to_graph(gjson, objectstore)
        else:
            g = jsonstr_to_graph(raw_execplan)
    return g;

gt_graph_pool = {}
kariz_graph_pool = {}

def load_graph_pools(gt_gp, kariz_gp):
    global gt_graph_pool
    global kariz_graph_pool
    gt_graph_pool = gt_gp
    kariz_graph_pool = kariz_gp


def graph_id_to_graph(g_spec, objectstore):
    g_name = g_spec['name']
    if g_name not in gt_graph_pool or g_name not in kariz_graph_pool:
        raise  NameError("Graph name %s is invalid"%(g_name))

    g_gt = gt_graph_pool[g_name].copy()
    g_kz = copy.deepcopy(kariz_graph_pool[g_name])

    g_kz.dag_id = g_spec['id']
    g_kz.name = g_name
    g_gt.gp.id = g_spec['id']
    for v in g_gt.vertices():
        g_kz.static_runtime(v, g_gt.vp.remote_runtime[v], g_gt.vp.cache_runtime[v])
        g_kz.config_inputs(v, g_gt.vp.inputdir[v])
    g_kz.queue_time = g_gt.gp.queue_time

    g_kz.g_gt = g_gt
    return g_kz


def pigstr_to_graph(raw_execplan, objectstore):
    ls = raw_execplan.split("\n")
    start_new_job = False
    v_index = -1
    vertices= {}
    vertices_size = {}
    for x in ls:
        if x.startswith('DAG'):
            dag_id = x.split(':')[1].replace('\'', '')
            
        if x.startswith("#"):
            continue;

        if x.startswith("MapReduce node:"):
            v_index = v_index + 1
            start_new_job = True
            vertices[v_index] = {}
    
        if x.find("Store") != -1:
            result = x.split('(')[1].split(')')[0]
            extra = result.split(":")[-1]
            results = result.replace(":" + extra, "")
            if 'output' not in vertices[v_index]:
                vertices[v_index]['output'] = {}
            outputs = results.split(',')
            for o in outputs:
               dataset_size, obj_name = objectstore.get_datasetsize_from_url(o)
               vertices[v_index]['output'][obj_name] = dataset_size
    
        if x.find("Load") != -1:
            result = x.split('(')[1].split(')')[0]
            extra = result.split(":")[-1]
            inputs =  result.replace(":" + extra, "")
            inputs = inputs.split(',')
            if 'inputs' not in vertices[v_index]:
                vertices[v_index]['inputs'] = {}
            for i in inputs:
               dataset_size, obj_name = objectstore.get_datasetsize_from_url(i)
               vertices[v_index]['inputs'][obj_name] = dataset_size

        if x.find("Quantile file") != -1:
            result = x.split('{')[1].split('}')[0]
            if 'inputs' not in vertices[v_index]:
                vertices[v_index]['inputs'] = {}
            inputs = result.split(',')
            for i in inputs:
                dataset_size, obj_name = objectstore.get_datasetsize_from_url(i)
                vertices[v_index]['inputs'][obj_name] = dataset_size


    g = Graph(len(vertices))
    g.dag_id = dag_id
    for v1 in vertices:
        for v2 in vertices:
            if v1 == v2: # and len(vertices) != 1:
                g.add_new_job(v1)
            
            g.config_inputs(v1, vertices[v1]['inputs'])

            for i in vertices[v1]['inputs']:
                if i in vertices[v2]['output']:
                    g.add_edge(v2, v1, 0)
    #print(str(g))
    return g


def jsonstr_to_graph(raw_execplan):
    raw_dag = ast.literal_eval(raw_execplan)
    jobs = raw_dag['jobs']
    n_vertices = raw_dag['n_vertices']
    g = Graph(n_vertices)
    g.dag_id = raw_dag['uuid']
    g.mse_factor = raw_dag['mse_factor']
    g.name = raw_dag['name']
    g.submit_time = raw_dag['submit_time']
    g.queue_time = raw_dag['queue_time']
    #g.total_runtime = raw_dag['total_runtime'] 
    for j in jobs:
        g.jobs[j['id']].id = j['id']
        g.jobs[j['id']].static_runtime(j['runtime_remote'], j['runtime_cache'])
        g.jobs[j['id']].set_misestimation(j['remote_misestimation'], j['cache_misestimation'])
        g.jobs[j['id']].config_ntasks(j['num_task'])
        g.config_inputs(j['id'], j['inputs']) 
        for ch in j['children']:
            g.add_edge(j['id'], ch, 0)
    g.config_misestimated_jobs()     
    return g


def build_input_format(inputs_str):
    res = re.search('\[(.*)\]', inputs_str)
    return dict.fromkeys(res.group(1).split('|'), 0) if res else {}
    

def build_graph_skeleton(g_str):
    g_elements = g_str.split('\n')
    g_name,g_type = g_elements[0].split(',')[1:]
    g_id = 0
    g_queuetime = 0
    
    g = gt.Graph(directed=True)
    g.gp['name'] = g.new_graph_property("string", g_name)
    g.gp['id'] = g.new_graph_property("string", str(g_id))
    g.gp['queue_time'] = g.new_graph_property("int", g_queuetime)
    g.gp['cur_stage'] = g.new_graph_property("int", -1)
    status = g.new_vertex_property("int")
    inputs = g.new_vertex_property("object")
    cache_runtime = g.new_vertex_property("int")
    remote_runtime = g.new_vertex_property("int")
    ops = g.new_vertex_property("vector<string>")

    # build vertices
    for el in g_elements[1:]:
        if el.startswith('v'):
            vid, inputs_str, operation = el.split(',')[1:]
            v = g.add_vertex()
            
            inputs[v] = build_input_format(inputs_str)
            cache_runtime[v] = 0
            remote_runtime[v] = 0 
            ops[v] = operation.split('|')
    
    # build edges
    for el in g_elements[1:]:
        if el.startswith('e'):
            v_src, v_dest = el.split(',')[1:]
            e = g.add_edge(v_src, v_dest)

    g.vp['inputs'] = inputs
    g.vp['remote_runtime'] = remote_runtime
    g.vp['cache_runtime'] = cache_runtime
    g.vp['status'] = status
    g.vp['ops'] = ops
    return g
            

def load_graph_skeleton(path):
    graph_skeletons = {}
    with open(path, 'r') as fd:
        graph_strs = fd.read().split('#')[1:]

        for g_str in graph_strs:
            g= build_graph_skeleton(g_str)
            graph_skeletons[g.gp.name] = g

    return graph_skeletons


def build_graph_from_gt(g_gt):
    g = Graph(g_gt.num_vertices())

    for v in g_gt.vertices():
        g.static_runtime(int(v), g_gt.vp.remote_runtime[v], g_gt.vp.cache_runtime[v])
        g.config_inputs(int(v), g_gt.vp.inputs[v])

    for e in g_gt.edges():
        g.add_edge(e.source(), e.target(), 0)

    g.name = g_gt.gp.name
    g.id = g_gt.gp.id
    return g


