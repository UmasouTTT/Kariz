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

# creating enumerations using class 
class Type(enum.IntEnum): 
    tiny = 0
    sequential = 1
    aggregate = 2
    broadcast = 3
    complex = 4

#Class to represent a graph 
class Graph: 
    def __init__(self, n_vertices = 0, type = Type.complex):
        self.dag_id = uuid.uuid1()
        self.n_vertices = n_vertices 
        self.jobs = {}
        for i in range(0, n_vertices):
            self.jobs[i] = jb.Job(i)
        
        self.misestimated_jobs = np.zeros(2*n_vertices)
        
        self.roots = set(range(0, n_vertices))
        self.leaves = set(range(0, n_vertices))
        self.blevels = {}
        
        self.mse_factor = 0
        self.plans_container = None
        self.stages = {}
        self.name = 'graph'
        self.category = type

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
        graph_str = graph_str  + '", "n_vertices" : ' + str(self.n_vertices) + ', "mse_factor" : ' + str(self.mse_factor) + ', "name" : "' + str(self.name) + '"}'  
        return graph_str

    def add_new_job(self, value):
        self.jobs[self.n_vertices] = jb.Job(self.n_vertices)
        self.n_vertices+= 1
    
    def set_misestimated_jobs(self, mse_jobs):
        for i in range(0, self.n_vertices):
            self.jobs[i].set_misestimation(mse_jobs[i], mse_jobs[i + self.n_vertices])
            
    def config_misestimated_jobs(self): # mse_factor: miss estimation factor
        for i in range(0, self.n_vertices):
            self.jobs[i].config_misestimated_runtimes(self.mse_factor)
    
    def set_misestimation_error(self, mse_factor):
        self.mse_factor = mse_factor;
    
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
            #j['job'].est_runtime_remote = j['job'].runtime_remote - j['improvement']


def str_to_graph(raw_execplan, objectstore):
    g = None
    if raw_execplan.startswith('DAG'):
        g = pigstr_to_graph(raw_execplan, objectstore)
    elif raw_execplan.startswith('ID'):
        g = graph_id_to_graph(raw_execplan, objectstore)
    else:
        g = jsonstr_to_graph(raw_execplan)
    return g;
        
def graph_id_to_graph(raw_execplan, objectstore):
    import framework_simulator.tpc as tpc  
    ls = raw_execplan.split(':')
    g_bench = ls[1]
    g_id = ls[2]
    g_ds = ls[3]
    if g_id in tpc.graphs_dict:
       g = tpc.graphs_dict[g_id]
       for j in g.jobs:
          for i in g.jobs[j].inputs:
              g.jobs[j].inputs[i] = objectstore.tpch_metadata[g_ds][i]
          g.static_runtime(j, objectstore.tpch_runtime[g_id][g_ds][j]['remote'], objectstore.tpch_runtime[g_id][g_ds][j]['cached'])
       return g

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
