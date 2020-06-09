#!/usr/bin/python

'''
Created on Sep 19, 2019

@author: mania
'''

import utils.graph as gr
from utils.inputs import *
import random
import graph_tool.all as gt
import ast
import os
import uuid

def prepare_tpc_metadata_wh(fpath):
    '''
    read metadata and build a heirarchy tree
    '''
    tpch_inputs = {'1G' : {}, '2G' : {}, '4G' : {}, '8G' : {}, '16G' : {},
                '32G' : {}, '48G' : {}, '64G' : {}, '80G' : {},
                '100G' : {}, '128G' : {}, '150G' : {}, '200G' : {},
                '256G' : {}, '300G' : {}}
 
    tpcds_inputs = {'1G' : {}, '2G' : {}, '4G' : {}, '8G' : {}, '16G' : {},
                '32G' : {}, '48G' : {}, '64G' : {}, '80G' : {},
                '100G' : {}, '128G' : {}, '150G' : {}, '200G' : {},
                '256G' : {}, '300G' : {}}
 
    df = pd.read_csv(fpath + 'inputs.csv')
    df['size'] = pd.to_numeric(df['size'])
    df['n_blocks'] = (df['size']/blocksize).apply(np.ceil).astype(int)
    working_set_size = df['n_blocks'].sum() # 393GB 
    for index, row in df.iterrows():
        dataset_meta = row['name'].split('_')
        query = dataset_meta[0]
        ds_sz = dataset_meta[1]
        input_name = row['name'].replace(query+'_'+ds_sz+'_', '')
      
        if query == 'tpcds':
            tpcds_inputs[ds_sz][input_name] = row['n_blocks']         
        elif query == 'tpch':
            tpch_inputs[ds_sz][input_name] = row['n_blocks']
  
    return tpch_inputs, tpcds_inputs;
 
def prepare_tpc_metadata():
    metadata = {}
    
    with open('/home/mania/Northeastern/MoC/Kariz/code/utils/inputs.csv', 'r') as fd:
        lines = fd.readlines()[1:];
        for ln in lines:
            name, size = ln.replace('\n', '').split(',')
            metadata[name] = math.ceil(int(size)/blocksize); 
    return metadata;


def prepare_tpc_runtimes(fpath):
    tpch_runtimes = {}
    tpcds_runtimes = {}
    with open(fpath + 'tpchjobs.csv') as csvfile:
        readCSV = csv.reader(csvfile, delimiter=',')
        headers = next(readCSV, None)
        for row in readCSV:
            query = row[0]
            dataset = row[4]
            jobid = int(row[2])
            rorc = row[6]
            runtime = int(row[3])
            if query not in tpch_runtimes:
                tpch_runtimes[query]={}
            if dataset not in tpch_runtimes[query]:
                tpch_runtimes[query][dataset] = {}
            if jobid not in tpch_runtimes[query][dataset]:
                tpch_runtimes[query][dataset][jobid] = {}
            if rorc == 'R':
                tpch_runtimes[query][dataset][jobid]['remote'] = runtime
            elif rorc == 'C': 
                tpch_runtimes[query][dataset][jobid]['cached'] = runtime
    return tpch_runtimes, tpcds_runtimes

def build_tpc_graphpool():
    graphs = []
    graphs_dict = {}
    inputs.update(prepare_tpc_metadata())
    
    q3ds = gr.Graph(6, type=gr.Type.sequential)
    q3ds.add_edge(0, 1, 0)
    q3ds.add_edge(1, 2, 0)
    q3ds.add_edge(2, 3, 0)
    q3ds.add_edge(3, 4, 0)
    q3ds.add_edge(4, 5, 0)
    q3ds.add_edge(5, 6, 0)
    q3ds.config_inputs(0, {'d': 0})
    graphs.append(q3ds)
    graphs_dict['DSQ3'] = q3ds
    
    q1h1 = gr.Graph(3, type=gr.Type.broadcast)
    q1h1.add_edge(0, 1, 0)
    q1h1.add_edge(0, 2, 0)
    q1h1.add_edge(1, 2, 0)
    q1h1.config_inputs(0, {'tpch_1G_lineitem': inputs['tpch_1G_lineitem']})
    q1h1.static_runtime(0, 350, 230)
    q1h1.static_runtime(1, 50, 50)
    q1h1.static_runtime(2, 50, 50)
    graphs.append(q1h1)
    graphs_dict['HQ1-1'] = q1h1
    
    q1h2 = gr.Graph(3, type=gr.Type.broadcast)
    q1h2.add_edge(0, 1, 0)
    q1h2.add_edge(0, 2, 0)
    q1h2.add_edge(1, 2, 0)
    q1h2.static_runtime(0, 650, 430)
    q1h2.static_runtime(1, 100, 100)
    q1h2.static_runtime(2, 50, 50)
    q1h2.config_inputs(0, {'tpch_2G_lineitem': inputs['tpch_2G_lineitem']})
    graphs.append(q1h2)
    graphs_dict['HQ1-2'] = q1h2
    
    q1h4 = gr.Graph(3, type=gr.Type.broadcast)
    q1h4.add_edge(0, 1, 0)
    q1h4.add_edge(0, 2, 0)
    q1h4.add_edge(1, 2, 0)
    q1h4.static_runtime(0, 700, 500)
    q1h4.static_runtime(1, 150, 150)
    q1h4.static_runtime(2, 50, 50)
    q1h4.config_inputs(0, {'tpch_4G_lineitem': inputs['tpch_4G_lineitem']})
    graphs.append(q1h4)
    graphs_dict['HQ1-4'] = q1h4
    
    q1h8 = gr.Graph(3, type=gr.Type.broadcast)
    q1h8.add_edge(0, 1, 0)
    q1h8.add_edge(0, 2, 0)
    q1h8.add_edge(1, 2, 0)
    q1h8.static_runtime(0, 750, 530)
    q1h8.static_runtime(1, 150, 150)
    q1h8.static_runtime(2, 50, 50)
    q1h8.config_inputs(0, {'tpch_8G_lineitem': inputs['tpch_4G_lineitem']})
    graphs.append(q1h8)
    graphs_dict['HQ1-8'] = q1h8
    
    q1h = gr.Graph(3, type=gr.Type.broadcast)
    q1h.add_edge(0, 1, 0)
    q1h.add_edge(0, 2, 0)
    q1h.add_edge(1, 2, 0)
    q1h.config_inputs(0, {'tpch_4G_lineitem': inputs['tpch_4G_lineitem']})
    q1h8.static_runtime(0, 680, 500)
    q1h8.static_runtime(1, 150, 150)
    q1h8.static_runtime(2, 50, 50)
    graphs.append(q1h)
    graphs_dict['HQ1'] = q1h
    
    
    q2h1 = gr.Graph(7, type=gr.Type.sequential)
    q2h1.add_edge(0, 1, 0)
    q2h1.add_edge(1, 2, 0)
    q2h1.add_edge(2, 3, 0)
    q2h1.add_edge(3, 4, 0)
    q2h1.add_edge(4, 5, 0)
    q2h1.add_edge(4, 6, 0)
    q2h1.add_edge(5, 6, 0)
    q2h1.config_inputs(0, {'tpch_1G_region': inputs['tpch_1G_region'], 'tpch_1G_nation': inputs['tpch_1G_nation']})
    q2h1.config_inputs(1, {'tpch_1G_supplier': inputs['tpch_1G_supplier']})
    q2h1.config_inputs(2, {'tpch_1G_partsupp': inputs['tpch_1G_partsupp']})
    q2h1.config_inputs(3, {'tpch_1G_part': inputs['tpch_1G_part']})
    graphs.append(q2h1)
    graphs_dict['HQ2'] = q2h1
    
    q2h2 = gr.Graph(7, type=gr.Type.sequential)
    q2h2.add_edge(0, 1, 0)
    q2h2.add_edge(1, 2, 0)
    q2h2.add_edge(2, 3, 0)
    q2h2.add_edge(3, 4, 0)
    q2h2.add_edge(4, 5, 0)
    q2h2.add_edge(4, 6, 0)
    q2h2.add_edge(5, 6, 0)
    q2h2.config_inputs(0, {'tpch_2G_region': inputs['tpch_2G_region'], 'tpch_2G_nation': inputs['tpch_2G_nation']})
    q2h2.config_inputs(1, {'tpch_2G_supplier': inputs['tpch_2G_supplier']})
    q2h2.config_inputs(2, {'tpch_2G_partsupp': inputs['tpch_2G_partsupp']})
    q2h2.config_inputs(3, {'tpch_2G_part': inputs['tpch_2G_part']})
    graphs.append(q2h2)
    graphs_dict['HQ2-2'] = q2h2
    
    
    q2h4 = gr.Graph(7, type=gr.Type.sequential)
    q2h4.add_edge(0, 1, 0)
    q2h4.add_edge(1, 2, 0)
    q2h4.add_edge(2, 3, 0)
    q2h4.add_edge(3, 4, 0)
    q2h4.add_edge(4, 5, 0)
    q2h4.add_edge(4, 6, 0)
    q2h4.add_edge(5, 6, 0)
    q2h4.config_inputs(0, {'tpch_4G_region': inputs['tpch_4G_region'], 'tpch_4G_nation': inputs['tpch_4G_nation']})
    q2h4.config_inputs(1, {'tpch_4G_supplier': inputs['tpch_4G_supplier']})
    q2h4.config_inputs(2, {'tpch_4G_partsupp': inputs['tpch_4G_partsupp']})
    q2h4.config_inputs(3, {'tpch_4G_part': inputs['tpch_4G_part']})
    graphs.append(q2h4)
    graphs_dict['HQ2-4'] = q2h4
    
    q2h8 = gr.Graph(7, type=gr.Type.sequential)
    q2h8.add_edge(0, 1, 0)
    q2h8.add_edge(1, 2, 0)
    q2h8.add_edge(2, 3, 0)
    q2h8.add_edge(3, 4, 0)
    q2h8.add_edge(4, 5, 0)
    q2h8.add_edge(4, 6, 0)
    q2h8.add_edge(5, 6, 0)
    q2h8.config_inputs(0, {'tpch_8G_region': inputs['tpch_8G_region'], 'tpch_8G_nation': inputs['tpch_8G_nation']})
    q2h8.config_inputs(1, {'tpch_8G_supplier': inputs['tpch_8G_supplier']})
    q2h8.config_inputs(2, {'tpch_8G_partsupp': inputs['tpch_8G_partsupp']})
    q2h8.config_inputs(3, {'tpch_8G_part': inputs['tpch_8G_part']})
    graphs.append(q2h8)
    graphs_dict['HQ2-8'] = q2h8
    
    
    q3h1 = gr.Graph(5,type=gr.Type.sequential)
    q3h1.add_edge(0, 1, 0)
    q3h1.add_edge(1, 2, 0)
    q3h1.add_edge(2, 3, 0)
    q3h1.add_edge(2, 4, 0)
    q3h1.add_edge(3, 4, 0)
    q3h1.config_inputs(0, {'tpch_1G_orders': inputs['tpch_1G_orders']})
    q3h1.config_inputs(1, {'tpch_1G_lineitem': inputs['tpch_1G_lineitem']})
    graphs.append(q3h1)
    graphs_dict['HQ3'] = q3h1
    
    q3h2 = gr.Graph(5,type=gr.Type.sequential)
    q3h2.add_edge(0, 1, 0)
    q3h2.add_edge(1, 2, 0)
    q3h2.add_edge(2, 3, 0)
    q3h2.add_edge(2, 4, 0)
    q3h2.add_edge(3, 4, 0)
    q3h2.config_inputs(0, {'tpch_2G_orders': inputs['tpch_2G_orders']})
    q3h2.config_inputs(1, {'tpch_2G_lineitem': inputs['tpch_2G_lineitem']})
    graphs.append(q3h2)
    graphs_dict['HQ3-2'] = q3h2
    
    q3h4 = gr.Graph(5,type=gr.Type.sequential)
    q3h4.add_edge(0, 1, 0)
    q3h4.add_edge(1, 2, 0)
    q3h4.add_edge(2, 3, 0)
    q3h4.add_edge(2, 4, 0)
    q3h4.add_edge(3, 4, 0)
    q3h4.config_inputs(0, {'tpch_4G_orders': inputs['tpch_4G_orders']})
    q3h4.config_inputs(1, {'tpch_4G_lineitem': inputs['tpch_4G_lineitem']})
    graphs.append(q3h4)
    graphs_dict['HQ3-1'] = q3h4
    
    q3h8 = gr.Graph(5,type=gr.Type.sequential)
    q3h8.add_edge(0, 1, 0)
    q3h8.add_edge(1, 2, 0)
    q3h8.add_edge(2, 3, 0)
    q3h8.add_edge(2, 4, 0)
    q3h8.add_edge(3, 4, 0)
    q3h8.config_inputs(0, {'tpch_8G_orders': inputs['tpch_8G_orders']})
    q3h8.config_inputs(1, {'tpch_8G_lineitem': inputs['tpch_8G_lineitem']})
    graphs.append(q3h8)
    graphs_dict['HQ3-1'] = q3h8
    
    
    q4h1 = gr.Graph(4, type=gr.Type.broadcast)
    q4h1.add_edge(0, 1, 0)
    q4h1.add_edge(1, 2, 0)
    q4h1.add_edge(1, 3, 0)
    q4h1.add_edge(3, 4, 0)
    q4h1.config_inputs(0, {'tpch_1G_orders': inputs['tpch_1G_orders'], 'tpch_1G_lineitem': inputs['tpch_1G_lineitem']})
    graphs.append(q4h1)
    graphs_dict['HQ4'] = q4h1
    
    
    q4h2 = gr.Graph(4, type=gr.Type.broadcast)
    q4h2.add_edge(0, 1, 0)
    q4h2.add_edge(1, 2, 0)
    q4h2.add_edge(1, 3, 0)
    q4h2.add_edge(3, 4, 0)
    q4h2.config_inputs(0, {'tpch_2G_orders': inputs['tpch_2G_orders'], 'tpch_2G_lineitem': inputs['tpch_2G_lineitem']})
    graphs.append(q4h2)
    graphs_dict['HQ4-2'] = q4h2
    
    q4h4 = gr.Graph(4, type=gr.Type.broadcast)
    q4h4.add_edge(0, 1, 0)
    q4h4.add_edge(1, 2, 0)
    q4h4.add_edge(1, 3, 0)
    q4h4.add_edge(3, 4, 0)
    q4h4.config_inputs(0, {'tpch_4G_orders': inputs['tpch_4G_orders'], 'tpch_4G_lineitem': inputs['tpch_4G_lineitem']})
    graphs.append(q4h4)
    graphs_dict['HQ4-4'] = q4h4
    
    
    q4h8 = gr.Graph(4, type=gr.Type.broadcast)
    q4h8.add_edge(0, 1, 0)
    q4h8.add_edge(1, 2, 0)
    q4h8.add_edge(1, 3, 0)
    q4h8.add_edge(3, 4, 0)
    q4h8.config_inputs(0, {'tpch_8G_orders': inputs['tpch_8G_orders'], 'tpch_8G_lineitem': inputs['tpch_8G_lineitem']})
    graphs.append(q4h8)
    graphs_dict['HQ4-8'] = q4h8
    
    
    q5h = gr.Graph(8, type=gr.Type.sequential)
    q5h.add_edge(0, 1, 0)
    q5h.add_edge(1, 2, 0)
    q5h.add_edge(2, 3, 0)
    q5h.add_edge(3, 4, 0)
    q5h.add_edge(4, 5, 0)
    q5h.add_edge(5, 6, 0)
    q5h.add_edge(5, 7, 0)
    q5h.add_edge(6, 7, 0)
    q5h.config_inputs(0, {'tpch_1G_nation': inputs['tpch_8G_orders'], 'tpch_1G_region': inputs['tpch_8G_orders']})
    q5h.config_inputs(1, {'tpch_1G_supplier': inputs['tpch_8G_orders']})
    q5h.config_inputs(2, {'tpch_1G_lineitem': inputs['tpch_8G_orders']})
    q5h.config_inputs(3, {'tpch_1G_orders': inputs['tpch_8G_orders']})
    q5h.config_inputs(4, {'tpch_1G_customer': inputs['tpch_8G_orders']})
    graphs.append(q5h)
    graphs_dict['HQ5'] = q5h
    
    q5h = gr.Graph(8, type=gr.Type.sequential)
    q5h.add_edge(0, 1, 0)
    q5h.add_edge(1, 2, 0)
    q5h.add_edge(2, 3, 0)
    q5h.add_edge(3, 4, 0)
    q5h.add_edge(4, 5, 0)
    q5h.add_edge(5, 6, 0)
    q5h.add_edge(5, 7, 0)
    q5h.add_edge(6, 7, 0)
    q5h.config_inputs(0, {'tpch_1G_nation': inputs['tpch_8G_orders'], 'tpch_1G_region': inputs['tpch_8G_orders']})
    q5h.config_inputs(1, {'tpch_1G_supplier': inputs['tpch_8G_orders']})
    q5h.config_inputs(2, {'tpch_1G_lineitem': inputs['tpch_8G_orders']})
    q5h.config_inputs(3, {'tpch_1G_orders': inputs['tpch_8G_orders']})
    q5h.config_inputs(4, {'tpch_1G_customer': inputs['tpch_8G_orders']})
    graphs.append(q5h)
    graphs_dict['HQ5'] = q5h
    
    q5h = gr.Graph(8, type=gr.Type.sequential)
    q5h.add_edge(0, 1, 0)
    q5h.add_edge(1, 2, 0)
    q5h.add_edge(2, 3, 0)
    q5h.add_edge(3, 4, 0)
    q5h.add_edge(4, 5, 0)
    q5h.add_edge(5, 6, 0)
    q5h.add_edge(5, 7, 0)
    q5h.add_edge(6, 7, 0)
    q5h.config_inputs(0, {'tpch_1G_nation': inputs['tpch_8G_orders'], 'tpch_1G_region': inputs['tpch_8G_orders']})
    q5h.config_inputs(1, {'tpch_1G_supplier': inputs['tpch_8G_orders']})
    q5h.config_inputs(2, {'tpch_1G_lineitem': inputs['tpch_8G_orders']})
    q5h.config_inputs(3, {'tpch_1G_orders': inputs['tpch_8G_orders']})
    q5h.config_inputs(4, {'tpch_1G_customer': inputs['tpch_8G_orders']})
    graphs.append(q5h)
    graphs_dict['HQ5'] = q5h
    
    q5h = gr.Graph(8, type=gr.Type.sequential)
    q5h.add_edge(0, 1, 0)
    q5h.add_edge(1, 2, 0)
    q5h.add_edge(2, 3, 0)
    q5h.add_edge(3, 4, 0)
    q5h.add_edge(4, 5, 0)
    q5h.add_edge(5, 6, 0)
    q5h.add_edge(5, 7, 0)
    q5h.add_edge(6, 7, 0)
    q5h.config_inputs(0, {'tpch_1G_nation': inputs['tpch_8G_orders'], 'tpch_1G_region': inputs['tpch_8G_orders']})
    q5h.config_inputs(1, {'tpch_1G_supplier': inputs['tpch_8G_orders']})
    q5h.config_inputs(2, {'tpch_1G_lineitem': inputs['tpch_8G_orders']})
    q5h.config_inputs(3, {'tpch_1G_orders': inputs['tpch_8G_orders']})
    q5h.config_inputs(4, {'tpch_1G_customer': inputs['tpch_8G_orders']})
    graphs.append(q5h)
    graphs_dict['HQ5'] = q5h
    
    q6h = gr.Graph(1, type=gr.Type.tiny)
    q6h.config_inputs(0, {'tpch_1G_lineitem': inputs['tpch_1G_lineitem']})
    q6h.static_runtime(0, 50, 23)
    graphs.append(q6h)
    graphs_dict['HQ6'] = q6h
    
    q6h = gr.Graph(1, type=gr.Type.tiny)
    q6h.config_inputs(0, {'tpch_1G_lineitem': inputs['tpch_1G_lineitem']})
    q6h.static_runtime(0, 50, 23)
    graphs.append(q6h)
    graphs_dict['HQ6'] = q6h
    
    q6h = gr.Graph(1, type=gr.Type.tiny)
    q6h.config_inputs(0, {'tpch_1G_lineitem': inputs['tpch_1G_lineitem']})
    q6h.static_runtime(0, 50, 23)
    graphs.append(q6h)
    graphs_dict['HQ6'] = q6h
    
    q6h = gr.Graph(1, type=gr.Type.tiny)
    q6h.config_inputs(0, {'tpch_1G_lineitem': inputs['tpch_1G_lineitem']})
    q6h.static_runtime(0, 50, 23)
    graphs.append(q6h)
    graphs_dict['HQ6'] = q6h
    
    
    q7h = gr.Graph(8, type=gr.Type.aggregate)
    q7h.add_edge(0, 2, 0)
    q7h.add_edge(1, 3, 0)
    q7h.add_edge(2, 4, 0)
    q7h.add_edge(3, 4, 0)
    q7h.add_edge(4, 5, 0)
    q7h.add_edge(5, 6, 0)
    q7h.add_edge(5, 7, 0)
    q7h.add_edge(6, 7, 0)
    q7h.config_inputs(0, {'tpch_1G_nation': inputs['tpch_8G_orders']})
    q7h.config_inputs(1, {'tpch_1G_nation': inputs['tpch_8G_orders']})
    q7h.config_inputs(2, {'tpch_1G_supplier': inputs['tpch_8G_orders'], 'tpch_1G_lineitem': inputs['tpch_8G_orders']})
    q7h.config_inputs(3, {'tpch_1G_orders': inputs['tpch_8G_orders'], 'tpch_1G_customer': inputs['tpch_8G_orders']})
    graphs.append(q7h)
    graphs_dict['HQ7'] = q7h
    
    
    q7h = gr.Graph(8, type=gr.Type.aggregate)
    q7h.add_edge(0, 2, 0)
    q7h.add_edge(1, 3, 0)
    q7h.add_edge(2, 4, 0)
    q7h.add_edge(3, 4, 0)
    q7h.add_edge(4, 5, 0)
    q7h.add_edge(5, 6, 0)
    q7h.add_edge(5, 7, 0)
    q7h.add_edge(6, 7, 0)
    q7h.config_inputs(0, {'tpch_1G_nation': 0})
    q7h.config_inputs(1, {'tpch_1G_nation': 0})
    q7h.config_inputs(2, {'tpch_1G_supplier': 0, 'tpch_1G_lineitem': 0})
    q7h.config_inputs(3, {'tpch_1G_orders': 0, 'tpch_1G_customer': 0})
    graphs.append(q7h)
    graphs_dict['HQ7'] = q7h
    
    
    q7h = gr.Graph(8, type=gr.Type.aggregate)
    q7h.add_edge(0, 2, 0)
    q7h.add_edge(1, 3, 0)
    q7h.add_edge(2, 4, 0)
    q7h.add_edge(3, 4, 0)
    q7h.add_edge(4, 5, 0)
    q7h.add_edge(5, 6, 0)
    q7h.add_edge(5, 7, 0)
    q7h.add_edge(6, 7, 0)
    q7h.config_inputs(0, {'tpch_1G_nation': 0})
    q7h.config_inputs(1, {'tpch_1G_nation': 0})
    q7h.config_inputs(2, {'tpch_1G_supplier': 0, 'tpch_1G_lineitem': 0})
    q7h.config_inputs(3, {'tpch_1G_orders': 0, 'tpch_1G_customer': 0})
    graphs.append(q7h)
    graphs_dict['HQ7'] = q7h
    
    
    q7h = gr.Graph(8, type=gr.Type.aggregate)
    q7h.add_edge(0, 2, 0)
    q7h.add_edge(1, 3, 0)
    q7h.add_edge(2, 4, 0)
    q7h.add_edge(3, 4, 0)
    q7h.add_edge(4, 5, 0)
    q7h.add_edge(5, 6, 0)
    q7h.add_edge(5, 7, 0)
    q7h.add_edge(6, 7, 0)
    q7h.config_inputs(0, {'tpch_1G_nation': 0})
    q7h.config_inputs(1, {'tpch_1G_nation': 0})
    q7h.config_inputs(2, {'tpch_1G_supplier': 0, 'tpch_1G_lineitem': 0})
    q7h.config_inputs(3, {'tpch_1G_orders': 0, 'tpch_1G_customer': 0})
    graphs.append(q7h)
    graphs_dict['HQ7'] = q7h
    
    
    q8h = gr.Graph(11, type=gr.Type.complex)
    q8h.add_edge(0, 3, 0)
    q8h.add_edge(0, 4, 0)
    q8h.add_edge(1, 4, 0)
    q8h.add_edge(2, 3, 0)
    q8h.add_edge(3, 5, 0)
    q8h.add_edge(4, 7, 0)
    q8h.add_edge(5, 7, 0)
    q8h.add_edge(6, 7, 0)
    q8h.add_edge(8, 9, 0)
    q8h.add_edge(8, 10, 0)
    q8h.add_edge(9, 10, 0)
    q8h.config_inputs(0, {'tpch_1G_region': 0})
    q8h.config_inputs(1, {'tpch_1G_part': 0, 'tpch_1G_lineitem': 0})
    q8h.config_inputs(3, {'tpch_1G_nation': 0})
    q8h.config_inputs(4, {'tpch_1G_supplier': 0})
    q8h.config_inputs(6, {'tpch_1G_customer': 0, 'tpch_1G_orders': 0})
    graphs.append(q8h)
    graphs_dict['HQ8'] = q8h
    
    
    q9h = gr.Graph(8, type=gr.Type.aggregate)
    q9h.add_edge(0, 2, 0)
    q9h.add_edge(1, 2, 0)
    q9h.add_edge(2, 3, 0)
    q9h.add_edge(3, 4, 0)
    q9h.add_edge(4, 5, 0)
    q9h.add_edge(5, 6, 0)
    q9h.add_edge(5, 7, 0)
    q9h.add_edge(6, 7, 0)
    q9h.config_inputs(0, {'tpch_1G_nation': 0})
    q9h.config_inputs(1, {'tpch_1G_lineitem': 0, 'tpch_1G_part': 0})
    q9h.config_inputs(2, {'tpch_1G_supplier': 0})
    q9h.config_inputs(3, {'tpch_1G_partsupp': 0})
    q9h.config_inputs(4, {'tpch_1G_orders': 0})
    graphs.append(q9h)
    graphs_dict['HQ9'] = q9h
    
    q10h = gr.Graph(6, type=gr.Type.sequential)
    q10h.add_edge(0, 1, 0)
    q10h.add_edge(1, 2, 0)
    q10h.add_edge(2, 3, 0)
    q10h.add_edge(3, 4, 0)
    q10h.add_edge(3, 5, 0)
    q10h.add_edge(4, 5, 0)
    q10h.config_inputs(0, {'tpch_1G_customer': 0, 'tpch_1G_orders': 0})
    q10h.config_inputs(1, {'tpch_1G_nation': 0})
    q10h.config_inputs(2, {'tpch_1G_lineitem': 0})
    graphs.append(q10h)
    graphs_dict['HQ10'] = q10h
    
    q11h = gr.Graph(6, type=gr.Type.broadcast)
    q11h.add_edge(0, 1, 0)
    q11h.add_edge(1, 2, 0)
    q11h.add_edge(1, 3, 0)
    q11h.add_edge(3, 4, 0)
    q11h.add_edge(3, 5, 0)
    q11h.add_edge(4, 5, 0)
    q11h.config_inputs(0, {'tpch_1G_nation': 0, 'tpch_1G_supplier': 0})
    q11h.config_inputs(1, {'tpch_1G_partsupp': 0})
    graphs.append(q11h)
    graphs_dict['HQ11'] = q11h
    
    
    q12h = gr.Graph(4, type=gr.Type.sequential)
    q12h.add_edge(0, 1, 0)
    q12h.add_edge(1, 2, 0)
    q12h.add_edge(1, 3, 0)
    q12h.add_edge(2, 3, 0)
    q12h.config_inputs(0, {'tpch_1G_lineitem': 0, 'tpch_2G_lineitem': 0})
    graphs.append(q12h)
    graphs_dict['HQ12'] = q12h
    
    q13h = gr.Graph(4, type=gr.Type.sequential)
    q13h.add_edge(0, 1, 0)
    q13h.add_edge(1, 2, 0)
    q13h.add_edge(1, 3, 0)
    q13h.add_edge(2, 3, 0)
    q13h.config_inputs(0, {'tpch_1G_customer': 0, 'tpch_1G_orders': 0})
    graphs.append(q13h)
    graphs_dict['HQ13'] = q13h
    
    q14h = gr.Graph(3, type=gr.Type.tiny)
    q14h.add_edge(0, 1, 0)
    q14h.add_edge(1, 2, 0)
    q14h.config_inputs(0, {'tpch_1G_part': 0, 'tpch_1G_lineitem': 0})
    graphs.append(q14h)
    graphs_dict['HQ14'] = q14h
    
    q15h = gr.Graph(6, type=gr.Type.complex)
    q15h.add_edge(0, 1, 0)
    q15h.add_edge(0, 2, 0)
    q15h.add_edge(2, 3, 0)
    q15h.add_edge(3, 4, 0)
    q15h.add_edge(3, 5, 0)
    q15h.add_edge(4, 5, 0)
    q15h.config_inputs(0, {'tpch_1G_lineitem': 0})
    q15h.config_inputs(2, {'tpch_1G_supplier': 0})
    graphs.append(q15h)
    graphs_dict['HQ15'] = q15h
    
    q16h = gr.Graph(5, type=gr.Type.aggregate)
    q16h.add_edge(0, 1, 0)
    q16h.add_edge(1, 2, 0)
    q16h.add_edge(2, 3, 0)
    q16h.add_edge(2, 4, 0)
    q16h.add_edge(3, 4, 0)
    q16h.config_inputs(0, {'tpch_1G_partsupp': 0, 'tpch_1G_supplier': 0})
    q16h.config_inputs(1, {'tpch_1G_part': 0})
    graphs.append(q16h)
    graphs_dict['HQ16'] = q16h
    
    q17h = gr.Graph(2, type=gr.Type.tiny)
    q17h.add_edge(0, 1, 0)
    q17h.add_edge(1, 2, 0)
    q17h.config_inputs(0, {'tpch_1G_lineitem': 0, 'tpch_1G_part': 0})
    graphs.append(q17h)
    graphs_dict['HQ17'] = q17h
    
    q18h = gr.Graph(6, type=gr.Type.complex)
    q18h.add_edge(0, 1, 0)
    q18h.add_edge(1, 2, 0)
    q18h.add_edge(2, 3, 0)
    q18h.add_edge(3, 4, 0)
    q18h.add_edge(3, 5, 0)
    q18h.add_edge(4, 5, 0)
    q18h.config_inputs(0, {'tpch_1G_lineitem': 0, 'tpch_1G_orders': 0})
    q18h.config_inputs(1, {'tpch_1G_customer': 0})
    graphs.append(q18h)
    graphs_dict['HQ18'] = q18h
    
    
    q19h = gr.Graph(2, type=gr.Type.tiny)
    q19h.add_edge(0, 1, 0)
    q19h.config_inputs(0, {'tpch_1G_lineitem': 0, 'tpch_1G_part': 0})
    graphs.append(q19h)
    graphs_dict['HQ19'] = q19h
    
    
    q20h = gr.Graph(9, type=gr.Type.aggregate)
    q20h.add_edge(0, 3, 0)
    q20h.add_edge(1, 3, 0)
    q20h.add_edge(2, 4, 0)
    q20h.add_edge(3, 4, 0)
    q20h.add_edge(4, 5, 0)
    q20h.add_edge(5, 6, 0)
    q20h.add_edge(6, 7, 0)
    q20h.add_edge(6, 8, 0)
    q20h.add_edge(7, 8, 0)
    q20h.config_inputs(0, {'tpch_1G_nation': 0, 'tpch_1G_supplier': 0})
    q20h.config_inputs(1, {'tpch_1G_part': 0})
    q20h.config_inputs(2, {'tpch_1G_lineitem': 0})
    q20h.config_inputs(3, {'tpch_1G_partsupp': 0})
    graphs.append(q20h)
    graphs_dict['HQ20'] = q20h
    
    q21h = gr.Graph(8, type=gr.Type.complex)
    q21h.add_edge(0, 2, 0)
    q21h.add_edge(1, 2, 0)
    q21h.add_edge(2, 3, 0)
    q21h.add_edge(3, 4, 0)
    q21h.add_edge(4, 5, 0)
    q21h.add_edge(5, 6, 0)
    q21h.add_edge(5, 7, 0)
    q21h.add_edge(6, 7, 0)
    q20h.config_inputs(0, {'tpch_1G_nation': 0})
    q20h.config_inputs(1, {'tpch_1G_lineitem': 0})
    q20h.config_inputs(3, {'tpch_1G_supplier': 0})
    q20h.config_inputs(4, {'tpch_1G_orders': 0})
    graphs.append(q21h)
    graphs_dict['HQ21'] = q21h
    
    q22h = gr.Graph(6, type=gr.Type.sequential)
    q22h.add_edge(0, 1, 0)
    q22h.add_edge(1, 2, 0)
    q22h.add_edge(2, 3, 0)
    q22h.add_edge(3, 4, 0)
    q22h.add_edge(3, 5, 0)
    q22h.add_edge(4, 5, 0)
    q22h.config_inputs(0, {'tpch_1G_customer': 0})
    q22h.config_inputs(1, {'tpch_1G_orders': 0})
    graphs.append(q22h)
    graphs_dict['HQ22'] = q22h
    
    ######################################################################
    q1a = gr.Graph(8, type=gr.Type.aggregate)
    q1a.add_edge(0, 3, 0)
    q1a.add_edge(1, 3, 0)
    q1a.add_edge(1, 4, 0)
    q1a.add_edge(2, 4, 0)
    q1a.add_edge(3, 5, 0)
    q1a.add_edge(4, 5, 0)
    q1a.add_edge(5, 6, 0)
    q1a.add_edge(6, 7, 0)
    q1a.static_runtime(0, 50, 23)
    q1a.static_runtime(1, 232, 77)
    q1a.static_runtime(2, 634, 100)
    q1a.static_runtime(3, 60, 40)
    q1a.static_runtime(4, 80, 50)
    q1a.static_runtime(5, 41, 41)
    q1a.static_runtime(6, 11, 11)
    q1a.static_runtime(7, 40, 40)
    q1a.config_inputs(0, {'a1': inputs['a1']})
    q1a.config_inputs(1, {'b23': inputs['b23'], 'c23': inputs['c23']})
    q1a.config_inputs(2, {'d23': inputs['d23'], 'e23': inputs['e23']})
    q1a.config_inputs(3, {'sup': inputs['sup']})
    q1a.config_inputs(4, {'ad': inputs['ad']})
    q1a.set_misestimated_jobs(np.array([1, 1, 0, 1, 0, 1, 1, 1, 0, 1, 1, 1, 1, 0, 0, 0]))
    graphs.append(q1a)
    graphs_dict['AQ1'] = q1a
    
    q2a = gr.Graph(11, type=gr.Type.complex)
    q2a.add_edge(0, 3, 0)
    q2a.add_edge(1, 3, 0)
    q2a.add_edge(1, 4, 0)
    q2a.add_edge(2, 4, 0)
    q2a.add_edge(4, 5, 0)
    q2a.add_edge(5, 6, 0)
    q2a.add_edge(5, 7, 0)
    q2a.add_edge(6, 7, 0)
    q2a.add_edge(3, 8, 0)
    q2a.add_edge(8, 9, 0)
    q2a.add_edge(9, 10, 0)
    q2a.static_runtime(0, 50, 23)
    q2a.static_runtime(1, 634, 200)
    q2a.static_runtime(2, 232, 77)
    q2a.static_runtime(3, 7, 2)
    q2a.static_runtime(4, 53, 17)
    q2a.static_runtime(5, 41, 41)
    q2a.static_runtime(6, 11, 11)
    q2a.static_runtime(7, 40, 40)
    q2a.static_runtime(8, 0, 0)
    q2a.static_runtime(9, 0, 0)
    q2a.static_runtime(10, 0, 0)
    q2a.config_inputs(0, {'d': inputs['d']})
    q2a.config_inputs(1, {'b7': inputs['b7'], 'li': inputs['li']})
    q2a.config_inputs(2, {'d7': inputs['d7'], 'c7': inputs['c7']})
    q2a.config_inputs(3, {'sup': inputs['sup']})
    q2a.config_inputs(4, {'ad': inputs['ad']})
    q2a.set_misestimated_jobs(np.array([1, 1, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0, 1, 1, 1, 0, 1, 0, 0, 0, 0, 0]))
    graphs.append(q2a)
    graphs_dict['AQ2'] = q2a
    
    q3a = gr.Graph(12, type=gr.Type.complex)
    q3a.add_edge(0, 2, 0)
    q3a.add_edge(0, 3, 0)
    q3a.add_edge(1, 4, 0)
    q3a.add_edge(3, 6, 0)
    q3a.add_edge(4, 7, 0)
    q3a.add_edge(5, 7, 0)
    q3a.add_edge(6, 9, 0)
    q3a.add_edge(7, 9, 0)
    q3a.add_edge(8, 9, 0)
    q3a.add_edge(9, 10, 0)
    q3a.add_edge(9, 11, 0)
    q3a.add_edge(10, 11, 0)
    q3a.static_runtime(0, 7, 2)
    q3a.static_runtime(1, 636, 212)
    q3a.static_runtime(2, 7, 2)
    q3a.static_runtime(3, 76, 25)
    q3a.static_runtime(4, 50, 20)
    q3a.static_runtime(5, 251, 83)
    q3a.static_runtime(6, 49, 16)
    q3a.static_runtime(7, 5, 1)
    q3a.static_runtime(8, 76, 25)
    q3a.static_runtime(9, 39, 13)
    q3a.static_runtime(10, 11, 3)
    q3a.static_runtime(11, 39, 13)
    q3a.config_inputs(0, {'na': inputs['na']})
    q3a.config_inputs(1, {'p': inputs['p'], 'li': inputs['li']})
    q3a.config_inputs(3, {'d': inputs['d']})
    q3a.config_inputs(4, {'sup': inputs['sup']})
    q3a.config_inputs(5, {'or': inputs['or'], 'cus': inputs['cus']})
    q3a.config_inputs(6, {'c2': inputs['c2']})
    q3a.config_inputs(8, {'re': inputs['re']})
    q3a.set_misestimated_jobs(np.array([0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 0, 1, 0, 1, 0, 1, 1,1]))
    graphs.append(q3a)
    graphs_dict['AQ3'] = q3a
    
    q4a = gr.Graph(8, type=gr.Type.aggregate)
    q4a.add_edge(0, 2, 0)
    q4a.add_edge(1, 2, 0)
    q4a.add_edge(2, 3, 0)
    q4a.add_edge(3, 4, 0)
    q4a.add_edge(4, 5, 0)
    q4a.add_edge(5, 6, 0)
    q4a.add_edge(5, 7, 0)
    q4a.add_edge(6, 7, 0)
    q4a.static_runtime(0, 7, 2)
    q4a.static_runtime(1, 645, 215)
    q4a.static_runtime(2, 90, 30)
    q4a.static_runtime(3, 172, 57)
    q4a.static_runtime(4, 205, 68)
    q4a.static_runtime(5, 43, 43)
    q4a.static_runtime(6, 11, 11)
    q4a.static_runtime(7, 41, 41)
    q4a.config_inputs(0, {'b7': inputs['b7']})
    q4a.config_inputs(1, {'p': inputs['p'], 'li': inputs['li']})
    q4a.config_inputs(2, {'ad': inputs['ad']})
    q4a.config_inputs(3, {'e4': inputs['e4']})
    q4a.config_inputs(4, {'f4': inputs['f4']})
    q4a.set_misestimated_jobs(np.array([1, 0, 0, 0, 0, 1, 1, 0, 0, 1, 1, 1, 0, 1, 1, 0]))
    graphs.append(q4a)
    graphs_dict['AQ4'] = q4a
    
    q5a = gr.Graph(6, type=gr.Type.sequential)
    q5a.add_edge(0, 1, 0)
    q5a.add_edge(1, 2, 0)
    q5a.add_edge(2, 3, 0)
    q5a.add_edge(3, 4, 0)
    q5a.add_edge(3, 5, 0)
    q5a.add_edge(4, 5, 0)
    q5a.static_runtime(0, 220, 73)
    q5a.static_runtime(1, 50, 16)
    q5a.static_runtime(2, 548, 182)
    q5a.static_runtime(3, 45, 45)
    q5a.static_runtime(4, 11, 11)
    q5a.static_runtime(5, 43, 43)
    q5a.config_inputs(0, {'a5': inputs['a5'], 'or': inputs['or']})
    q5a.config_inputs(1, {'b5': inputs['b5']})
    q5a.config_inputs(2, {'li': inputs['li']})
    q5a.set_misestimated_jobs(np.array([0, 1, 1, 1, 0, 1, 0, 1, 1, 0, 0, 0]))
    graphs.append(q5a)
    graphs_dict['AQ5'] = q5a
    
    q6a = gr.Graph(6, type=gr.Type.broadcast)
    q6a.add_edge(0, 1, 0)
    q6a.add_edge(1, 2, 0)
    q6a.add_edge(1, 3, 0)
    q6a.add_edge(3, 4, 0)
    q6a.add_edge(3, 5, 0)
    q6a.add_edge(4, 5, 0)
    q6a.static_runtime(0, 77, 25)
    q6a.static_runtime(1, 153, 51)
    q6a.static_runtime(2, 12, 4)
    q6a.static_runtime(3, 40, 13)
    q6a.static_runtime(4, 11, 11)
    q6a.static_runtime(5, 40, 40)
    q6a.config_inputs(0, {'sup': inputs['sup'], 'b6': inputs['b6']})
    q6a.config_inputs(1, {'c6': inputs['c6']})
    q6a.config_inputs(2, {'ab': inputs['ab']})
    q6a.config_inputs(3, {'d6': inputs['d6']})
    q6a.set_misestimated_jobs(np.array([0, 1, 0, 0, 0, 0, 0, 1, 1, 0, 1, 0]))
    graphs.append(q6a)
    graphs_dict['AQ6'] = q6a
    
    q7a = gr.Graph(4, type=gr.Type.broadcast)
    q7a.add_edge(0, 1, 0)
    q7a.add_edge(1, 2, 0)
    q7a.add_edge(1, 3, 0)
    q7a.add_edge(2, 3, 0)
    q7a.static_runtime(0, 667, 222)
    q7a.static_runtime(1, 41, 13)
    q7a.static_runtime(2, 11, 11)
    q7a.static_runtime(3, 39, 39)
    q7a.config_inputs(0, {'li2': inputs['li2'], 'f4': inputs['f4']})
    q7a.config_inputs(0, {'g': inputs['g']})
    q7a.set_misestimated_jobs(np.array([0, 1, 0, 0, 0, 1, 0, 0]))
    graphs.append(q7a)
    graphs_dict['AQ7'] = q7a
    
    
    q8a = gr.Graph(4, type=gr.Type.broadcast)
    q8a.add_edge(0, 1, 0)
    q8a.add_edge(0, 2, 0)
    q8a.add_edge(2, 3, 0)
    q8a.add_edge(1, 3, 0)
    q8a.static_runtime(0, 16, 12)
    q8a.static_runtime(1, 16, 8)
    q8a.static_runtime(2, 12, 8)
    q8a.static_runtime(3, 16, 8)
    q8a.config_inputs(0, {'a1': inputs['a1']})
    q8a.config_inputs(1, {'a2': inputs['a2']})
    q8a.config_inputs(2, {'a3': inputs['a3']})
    q8a.config_inputs(3, {'a4': inputs['a4']})
    q8a.set_misestimated_jobs(np.array([1, 0, 0, 0, 0, 0, 1, 0]))
    graphs.append(q8a)
    graphs_dict['AQ8'] = q8a
    
    q9a = gr.Graph(4, type=gr.Type.aggregate)
    q9a.add_edge(0, 2, 0)
    q9a.add_edge(1, 2, 0)
    q9a.add_edge(2, 3, 0)
    q9a.static_runtime(0, 16, 8)
    q9a.static_runtime(1, 12, 8)
    q9a.static_runtime(2, 16, 12)
    q9a.static_runtime(3, 16, 8)
    q9a.config_inputs(0, {'b1': inputs['b1']})
    q9a.config_inputs(1, {'b2': inputs['b2']})
    q9a.config_inputs(2, {'b3': inputs['b3']})
    q9a.config_inputs(3, {'b4': inputs['b4']})
    q9a.set_misestimated_jobs(np.array([1, 1, 0, 1, 1, 1, 1, 1]))
    graphs.append(q9a)
    graphs_dict['AQ9'] = q9a
    
    q10a = gr.Graph(10, type=gr.Type.complex)
    q10a.add_edge(0, 1, 0)
    q10a.add_edge(0, 3, 0)
    q10a.add_edge(1, 9, 0)
    q10a.add_edge(2, 4, 0)
    q10a.add_edge(2, 5, 0)
    q10a.add_edge(3, 4, 0)
    q10a.add_edge(5, 6, 0)
    q10a.add_edge(5, 7, 0)
    q10a.add_edge(4, 8, 0)
    q10a.add_edge(9, 8, 0)
    q10a.static_runtime(0, 670, 233)
    q10a.static_runtime(1, 188, 60)
    q10a.static_runtime(2, 170, 65)
    q10a.static_runtime(3, 200, 70)
    q10a.static_runtime(4, 188, 62)
    q10a.static_runtime(5, 115, 38)
    q10a.static_runtime(6, 39, 13)
    q10a.static_runtime(7, 40, 13)
    q10a.static_runtime(8, 0, 0)
    q10a.static_runtime(9, 0, 0)
    q10a.config_inputs(0, {'aa': inputs['aa']})
    q10a.config_inputs(1, {'ab': inputs['ab']})
    q10a.config_inputs(2, {'ad': inputs['ad']})
    q10a.config_inputs(3, {'ac': inputs['ac']})
    q10a.config_inputs(4, {'f': inputs['f']})
    q10a.config_inputs(5, {'g': inputs['g']})
    q10a.config_inputs(6, {'h': inputs['h']})
    q10a.config_inputs(7, {'b': inputs['b']})
    q10a.set_misestimated_jobs(np.array([1, 1, 1, 0, 0, 0, 0, 0, 1, 1, 1, 0, 0, 0, 0, 0, 0, 0, 1, 1]))
    graphs.append(q10a)
    graphs_dict['AQ10'] = q10a
    
    q11a = gr.Graph(7, type=gr.Type.complex)
    q11a.add_edge(0, 1, 0)
    q11a.add_edge(0, 2, 0)
    q11a.add_edge(1, 6, 0)
    q11a.add_edge(2, 3, 0)
    q11a.add_edge(3, 4, 0)
    q11a.add_edge(4, 5, 0)
    q11a.add_edge(6, 4, 0)
    q11a.static_runtime(0, 250, 80)
    q11a.static_runtime(1, 300, 100)
    q11a.static_runtime(2, 170, 75)
    q11a.static_runtime(3, 58, 19)
    q11a.static_runtime(4, 140, 50)
    q11a.static_runtime(5, 40, 40)
    q11a.static_runtime(6, 0, 0)
    q11a.config_inputs(0, {'a': inputs['a']})
    q11a.config_inputs(1, {'b': inputs['b']})
    q11a.config_inputs(2, {'c': inputs['c']})
    q11a.config_inputs(3, {'d': inputs['d']})
    q11a.config_inputs(4, {'b': inputs['b']})
    q11a.set_misestimated_jobs(np.array([0, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 0, 0, 1]))
    graphs.append(q11a)
    graphs_dict['AQ11'] = q11a
    
    q12a = gr.Graph(4, type=gr.Type.complex)
    q12a.add_edge(0, 1, 0)
    q12a.add_edge(1, 2, 0)
    q12a.add_edge(1, 3, 0)
    q12a.add_edge(2, 3, 0)
    q12a.static_runtime(0, 240, 80)
    q12a.static_runtime(1, 40, 13)
    q12a.static_runtime(2, 11, 3)
    q12a.static_runtime(3, 39, 13)
    q12a.config_inputs(0, {'f4': inputs['f4'], 'a5': inputs['a5']})
    q12a.set_misestimated_jobs(np.array([1, 0, 1, 0, 1, 0, 0, 0]))
    graphs.append(q12a)
    graphs_dict['AQ12'] = q12a
    
    q13a = gr.Graph(4, type=gr.Type.complex)
    q13a.add_edge(0, 1, 0)
    q13a.add_edge(0, 2, 0)
    q13a.add_edge(1, 3, 0)
    q13a.add_edge(2, 3, 0)
    q13a.static_runtime(0, 350, 210)
    q13a.static_runtime(1, 25, 13)
    q13a.static_runtime(2, 120, 48)
    q13a.static_runtime(3, 200, 140)
    q13a.config_inputs(0, {'ab': inputs['ab']})
    q13a.config_inputs(1, {'g': inputs['g']})
    q13a.config_inputs(2, {'a13': inputs['a13']})
    q13a.config_inputs(3, {'b23': inputs['b23'], 'b13': inputs['b13']})
    q13a.set_misestimated_jobs(np.array([0, 0, 1, 0, 0, 1, 0, 0]))
    graphs.append(q13a)
    graphs_dict['AQ13'] = q13a
    
    q14a = gr.Graph(5, type=gr.Type.complex)
    q14a.add_edge(0, 1, 0)
    q14a.add_edge(0, 2, 0)
    q14a.add_edge(1, 3, 0)
    q14a.add_edge(2, 3, 0)
    q14a.add_edge(3, 4, 0)
    q14a.static_runtime(0, 80, 50)
    q14a.static_runtime(1, 100, 40)
    q14a.static_runtime(2, 80, 60)
    q14a.static_runtime(3, 50, 50)
    q14a.static_runtime(4, 49, 49)
    q14a.config_inputs(0, {'c23': inputs['c23'], 'f': inputs['f']})
    q14a.config_inputs(1, {'a14': inputs['a14']})
    q14a.config_inputs(2, {'d6': inputs['d6']})
    q14a.set_misestimated_jobs(np.array([1, 1, 1, 1, 0, 1, 1, 0, 1, 1]))
    graphs.append(q14a)
    graphs_dict['AQ14'] = q14a
    
    q15a = gr.Graph(8, type=gr.Type.complex)
    q15a.add_edge(0, 3, 0)
    q15a.add_edge(1, 3, 0)
    q15a.add_edge(1, 4, 0)
    q15a.add_edge(2, 4, 0)
    q15a.add_edge(3, 5, 0)
    q15a.add_edge(4, 5, 0)
    q15a.add_edge(5, 6, 0)
    q15a.add_edge(6, 7, 0)
    q15a.static_runtime(0, 50, 23)
    q15a.static_runtime(1, 232, 77)
    q15a.static_runtime(2, 634, 200)
    q15a.static_runtime(3, 60, 40)
    q15a.static_runtime(4, 80, 50)
    q15a.static_runtime(5, 41, 41)
    q15a.static_runtime(6, 11, 11)
    q15a.static_runtime(7, 40, 40)
    q15a.config_inputs(0, {'a1': inputs['a1']})
    q15a.config_inputs(1, {'b23': inputs['b23'], 'c23': inputs['c23']})
    q15a.config_inputs(2, {'d23': inputs['d23'], 'e23': inputs['e23']})
    q15a.config_inputs(3, {'sup': inputs['sup']})
    q15a.config_inputs(4, {'ad': inputs['ad']})
    q15a.set_misestimated_jobs(np.array([0, 0, 1, 0, 1, 0, 1, 0, 1, 1, 0, 1, 0, 1, 0, 1]))
    graphs.append(q15a)
    graphs_dict['AQ15'] = q15a
    
    q16a = gr.Graph(6, type=gr.Type.complex)
    q16a.add_edge(0, 1, 0)
    q16a.add_edge(0, 2, 0)
    q16a.add_edge(0, 3, 0)
    q16a.add_edge(1, 4, 0)
    q16a.add_edge(2, 4, 0)
    q16a.add_edge(3, 5, 0)
    q16a.static_runtime(0, 400, 23)
    q16a.static_runtime(1, 250, 175)
    q16a.static_runtime(2, 300, 125)
    q16a.static_runtime(3, 275, 150)
    q16a.static_runtime(4, 80, 45)
    q16a.static_runtime(5, 80, 35)
    q16a.config_inputs(0, {'a16': inputs['a16']})
    q16a.config_inputs(1, {'b16': inputs['b16']})
    q16a.config_inputs(2, {'c23': inputs['c23']})
    q16a.config_inputs(3, {'c16': inputs['c16']})
    q16a.config_inputs(4, {'d16': inputs['d16']})
    q16a.config_inputs(5, {'e16': inputs['e16']})
    q16a.set_misestimated_jobs(np.array([0, 1, 0, 0, 0, 1, 1, 1, 1, 1, 1, 0]))
    graphs.append(q16a)
    graphs_dict['AQ16'] = q16a
    
    q17a = gr.Graph(6, type=gr.Type.complex)
    q17a.add_edge(0, 1, 0)
    q17a.add_edge(0, 2, 0)
    q17a.add_edge(0, 3, 0)
    q17a.add_edge(1, 4, 0)
    q17a.add_edge(2, 4, 0)
    q17a.add_edge(3, 5, 0)
    q17a.static_runtime(0, 735, 245)
    q17a.static_runtime(1, 550, 300)
    q17a.static_runtime(2, 90, 50)
    q17a.static_runtime(3, 110, 70)
    q17a.static_runtime(4, 130, 70)
    q17a.static_runtime(5, 240, 130)
    q17a.config_inputs(0, {'li': inputs['li'], 'f4': inputs['f4']})
    q17a.config_inputs(1, {'d6': inputs['d6'], 'a18': inputs['a18']})
    q17a.config_inputs(2, {'b18': inputs['b18']})
    q17a.config_inputs(3, {'d': inputs['d']})
    q17a.config_inputs(4, {'f': inputs['f']})
    q17a.config_inputs(5, {'g': inputs['g']})
    q17a.set_misestimated_jobs(np.array([0, 1, 0, 0, 1, 0, 1, 0, 0, 0, 1, 1]))
    graphs.append(q17a)
    graphs_dict['AQ17'] = q17a
    
    q18a = gr.Graph(3, type=gr.Type.aggregate)
    q18a.add_edge(0, 2, 0)
    q18a.add_edge(1, 2, 0)
    q18a.static_runtime(0, 400, 240)
    q18a.static_runtime(1, 420, 320)
    q18a.static_runtime(2, 200, 100)
    q18a.config_inputs(0, {'a18': inputs['a18']})
    q18a.config_inputs(1, {'d6': inputs['d6']})
    q18a.config_inputs(2, {'a14': inputs['a14']})
    q18a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q18a)
    graphs_dict['AQ18'] = q18a
    
    q19a = gr.Graph(3, type=gr.Type.aggregate)
    q19a.add_edge(0, 2, 0)
    q19a.add_edge(1, 2, 0)
    q19a.static_runtime(0, 128, 100)
    q19a.static_runtime(1, 100, 89)
    q19a.static_runtime(2, 50, 36)
    q19a.config_ntasks(0, 5)
    q19a.config_ntasks(1, 3)
    q19a.config_ntasks(2, 4)
    q19a.config_operation(0, 'wordcount')
    q19a.config_operation(1, 'wordcount')
    q19a.config_operation(2, 'wordcount')
    q19a.config_inputs(0, {'wc5G': inputs['wc5G']})
    q19a.config_inputs(1, {'wc3G': inputs['wc3G']})
    q19a.config_inputs(2, {'wc4G': inputs['wc4G']})
    q19a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q19a)
    graphs_dict['AQ19'] = q19a
    
    
    q20a = gr.Graph(6, type=gr.Type.aggregate)
    q20a.add_edge(0, 2, 0)
    q20a.add_edge(1, 2, 0)
    q20a.add_edge(2, 4, 0)
    q20a.add_edge(3, 4, 0)
    q20a.add_edge(4, 5, 0)
    q20a.static_runtime(0, 240, 150)
    q20a.static_runtime(1, 300, 150)
    q20a.static_runtime(2, 160, 120)
    q20a.static_runtime(3, 240, 120)
    q20a.static_runtime(4, 120, 100)
    q20a.static_runtime(5, 50, 50)
    q20a.config_inputs(0, {'a20': inputs['a20']})
    q20a.config_inputs(1, {'b20': inputs['b20']})
    q20a.config_inputs(2, {'c20': inputs['c20']})
    q20a.config_inputs(3, {'d20': inputs['d20']})
    q20a.config_inputs(4, {'e20': inputs['e20']})
    #q20a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q20a)
    graphs_dict['AQ20'] = q20a
    
    q21a = gr.Graph(6, type=gr.Type.aggregate)
    q21a.add_edge(0, 2, 0)
    q21a.add_edge(1, 2, 0)
    q21a.add_edge(2, 4, 0)
    q21a.add_edge(3, 4, 0)
    q21a.add_edge(4, 5, 0)
    q21a.static_runtime(0, 240, 150)
    q21a.static_runtime(1, 300, 150)
    q21a.static_runtime(2, 160, 120)
    q21a.static_runtime(3, 240, 120)
    q21a.static_runtime(4, 120, 100)
    q21a.static_runtime(5, 50, 50)
    q21a.config_inputs(0, {'a21': inputs['a21']})
    q21a.config_inputs(1, {'a20': inputs['a20']})
    q21a.config_inputs(2, {'c21': inputs['c21']})
    q21a.config_inputs(3, {'d21': inputs['d21']})
    q21a.config_inputs(4, {'e20': inputs['e20']})
    #q21a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q21a)
    graphs_dict['AQ21'] = q21a
    
    
    q22a = gr.Graph(6, type=gr.Type.aggregate)
    q22a.add_edge(0, 2, 0)
    q22a.add_edge(1, 2, 0)
    q22a.add_edge(2, 4, 0)
    q22a.add_edge(3, 4, 0)
    q22a.add_edge(4, 5, 0)
    q22a.static_runtime(0, 240, 150)
    q22a.static_runtime(1, 300, 150)
    q22a.static_runtime(2, 160, 120)
    q22a.static_runtime(3, 240, 120)
    q22a.static_runtime(4, 120, 100)
    q22a.static_runtime(5, 50, 50)
    q22a.config_inputs(0, {'a20': inputs['a20']})
    q22a.config_inputs(1, {'b21': inputs['b21']})
    q22a.config_inputs(2, {'c20': inputs['c20']})
    q22a.config_inputs(3, {'d21': inputs['d21']})
    q22a.config_inputs(4, {'e22': inputs['e22']})
    #q22a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q22a)
    graphs_dict['AQ22'] = q22a
    
    
    q23a = gr.Graph(6, type=gr.Type.aggregate)
    q23a.add_edge(0, 2, 0)
    q23a.add_edge(1, 2, 0)
    q23a.add_edge(2, 4, 0)
    q23a.add_edge(3, 4, 0)
    q23a.add_edge(4, 5, 0)
    q23a.static_runtime(0, 240, 150)
    q23a.static_runtime(1, 300, 150)
    q23a.static_runtime(2, 160, 120)
    q23a.static_runtime(3, 240, 120)
    q23a.static_runtime(4, 120, 100)
    q23a.static_runtime(5, 50, 50)
    q23a.config_inputs(0, {'a20': inputs['a20']})
    q23a.config_inputs(1, {'b20': inputs['b20']})
    q23a.config_inputs(2, {'c21': inputs['c21']})
    q23a.config_inputs(3, {'d21': inputs['d21']})
    q23a.config_inputs(4, {'e22': inputs['e22']})
    #q23a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q23a)
    graphs_dict['AQ23'] = q23a
    
    
    
    q24a = gr.Graph(6, type=gr.Type.aggregate)
    q24a.add_edge(0, 1, 0)
    q24a.add_edge(1, 2, 0)
    q24a.add_edge(2, 3, 0)
    q24a.add_edge(3, 4, 0)
    q24a.add_edge(4, 5, 0)
    q24a.static_runtime(0, 60, 40)
    q24a.static_runtime(1, 50, 40)
    q24a.static_runtime(2, 40, 20)
    q24a.static_runtime(3, 30, 20)
    q24a.static_runtime(4, 20, 10)
    q24a.static_runtime(5, 10, 10)
    q24a.config_inputs(0, {'a2': inputs['a2']})
    q24a.config_inputs(1, {'c23': inputs['b23']})
    q24a.config_inputs(2, {'b16': inputs['c16']})
    q24a.config_inputs(3, {'d6': inputs['d6']})
    q24a.config_inputs(4, {'c21': inputs['c21']})
    #q24a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q24a)
    graphs_dict['AQ24'] = q24a
    
    
    q25a = gr.Graph(6, type=gr.Type.aggregate, name='AQ25')
    q25a.add_edge(0, 1, 0)
    q25a.add_edge(1, 2, 0)
    q25a.add_edge(2, 3, 0)
    q25a.add_edge(3, 4, 0)
    q25a.add_edge(4, 5, 0)
    q25a.static_runtime(0, 60, 40)
    q25a.static_runtime(1, 50, 40)
    q25a.static_runtime(2, 40, 20)
    q25a.static_runtime(3, 30, 20)
    q25a.static_runtime(4, 20, 10)
    q25a.static_runtime(5, 10, 10)
    q25a.config_inputs(0, {'a2': inputs['a2']})
    q25a.config_inputs(1, {'c23': inputs['b23']})
    q25a.config_inputs(2, {'b16': inputs['c16']})
    q25a.config_inputs(3, {'d25': inputs['d25']})
    q25a.config_inputs(4, {'c21': inputs['c21']})
    #q24a.set_misestimated_jobs(np.array([0, 1, 1, 0, 1, 0]))
    graphs.append(q25a)
    graphs_dict['AQ25'] = q25a
    
    
    q26a = gr.Graph(4, type=gr.Type.aggregate, name='AQ26')
    q26a.add_edge(0, 3, 0)
    q26a.add_edge(1, 3, 0)
    q26a.add_edge(2, 3, 0)
    q26a.static_runtime(0, 200, 140)
    q26a.static_runtime(1, 260, 160)
    q26a.static_runtime(2, 280, 160)
    q26a.static_runtime(3, 30, 30)
    q26a.config_inputs(0, {'d21': inputs['d21']})
    q26a.config_inputs(1, {'f': inputs['f']})
    q26a.config_inputs(2, {'c26': inputs['c26']})
    graphs.append(q26a)
    graphs_dict['AQ26'] = q26a
    
    
    
    q27a = gr.Graph(9, type=gr.Type.aggregate, name='AQ27')
    q27a.add_edge(0, 3, 0)
    q27a.add_edge(1, 3, 0)
    q27a.add_edge(1, 4, 0)
    q27a.add_edge(2, 4, 0)
    q27a.add_edge(3, 6, 0)
    q27a.add_edge(4, 6, 0)
    q27a.add_edge(4, 7, 0)
    q27a.add_edge(5, 7, 0)
    q27a.add_edge(6, 8, 0)
    q27a.add_edge(7, 8, 0)
    q27a.static_runtime(0, 200, 140)
    q27a.static_runtime(1, 260, 160)
    q27a.static_runtime(2, 280, 160)
    q27a.static_runtime(3, 200, 150)
    q27a.static_runtime(4, 240, 160)
    q27a.static_runtime(5, 280, 150)
    q27a.static_runtime(6, 150, 100)
    q27a.static_runtime(7, 160, 120)
    q27a.static_runtime(8, 50, 50)
    q27a.config_inputs(0, {'c26': inputs['c26']})
    q27a.config_inputs(1, {'b3': inputs['b3']})
    q27a.config_inputs(2, {'a27': inputs['a27']})
    q27a.config_inputs(3, {'a18': inputs['a18']})
    q27a.config_inputs(4, {'b16': inputs['b16']})
    q27a.config_inputs(5, {'d6': inputs['d6']})
    q27a.config_inputs(6, {'d20': inputs['d20']})
    q27a.config_inputs(7, {'d': inputs['d']})
    graphs.append(q27a)
    graphs_dict['AQ27'] = q27a


    q28a = gr.Graph(6, type=gr.Type.sequential, name='AQ28')
    q28a.add_edge(0, 1, 0)
    q28a.add_edge(1, 2, 0)
    q28a.add_edge(2, 3, 0)
    q28a.add_edge(3, 4, 0)
    q28a.add_edge(4, 5, 0)
    q28a.static_runtime(0, 350, 200)
    q28a.static_runtime(1, 300, 200)
    q28a.static_runtime(2, 400, 200)
    q28a.static_runtime(3, 250, 200)
    q28a.static_runtime(4, 30, 30)
    q28a.static_runtime(5, 50, 50)
    q28a.config_inputs(0, {'a28': inputs['a28']})
    q28a.config_inputs(1, {'b28': inputs['b28']})
    q28a.config_inputs(2, {'c28': inputs['c28']})
    q28a.config_inputs(3, {'d28': inputs['d28']})
    graphs.append(q28a)
    graphs_dict['AQ28'] = q28a

    
    return graphs_dict;




def convert_to_graphtool(graphs_dic):
    cur_dir = os.getcwd()
    with open(cur_dir + '/synthetic_dags.g', 'w') as fd:
        graph_str = ''
        for dag_id in graphs_dic:
          dag = graphs_dic[dag_id];
          graph_str += ('# t,%s,%d,%d\n'%(dag_id, dag.category,random.randint(2, 10)))
          for jid in dag.jobs:
              job = dag.jobs[jid]
              if job.runtime_cache == 0 or job.runtime_remote == 0:
                  if len(job.inputs) == 0:
                      job.runtime_cache = random.randint(10, 70);
                      job.runtime_remote = job.runtime_cache; 
                  else:
                      job.runtime_remote = random.randint(10, 600);
                      job.runtime_cache = int(random.randint(1, 100)*job.runtime_remote/100); 
              
              for inp in job.inputs:
                  job.inputs[inp] = inputs[inp]
              graph_str += ('v,%d,%d,%d,%s\n'%(jid, job.runtime_remote, job.runtime_cache, json.dumps(job.inputs)))
          
          for jid in dag.jobs:
              job = dag.jobs[jid]
              for ch in job.children:
                  graph_str += ('e,%d,%d\n'%(jid, ch))
        fd.write(graph_str)
    
def build_dag_from_str(g_str):
    g_name, g_category, g_queuetime = g_str.split('\n')[0].split(',')[1:]
    g_id = uuid.uuid1()
    g_elements=g_str.split('\n')[:-1]
    g = gt.Graph(directed=True)
    g.gp['name'] = g.new_graph_property("string", g_name)
    g.gp['id'] = g.new_graph_property("string", str(g_id))
    g.gp['queue_time'] = g.new_graph_property("int", g_queuetime)
    g.gp['cur_stage'] = g.new_graph_property("int", -1)
    status = g.new_vertex_property("int")
    inputs = g.new_vertex_property("object")
    cache_runtime = g.new_vertex_property("int")
    remote_runtime = g.new_vertex_property("int")

    for g_e in g_elements:
        command, params = g_e.split(',', 1)

        if command == 'v':
            params, input_dirs = params.split('{')
            vid,r_rt, c_rt = params.split(',')[:-1]
            v = g.add_vertex()
            status[v] = 0
            inputs[v] = ast.literal_eval('{' + input_dirs)
            cache_runtime[v] = int(c_rt)
            remote_runtime[v] = int(r_rt)
        elif command == 'e':
            src, dest = params.split(',')
            g.add_edge(src, dest)
    g.vp['status'] = status;
    g.vp['inputdir'] = inputs
    g.vp['cache_runtime'] = cache_runtime
    g.vp['remote_runtime'] = remote_runtime
    return g;

    
    
def load_synthetic_graphs():
    with open('/home/mania/Northeastern/MoC/Kariz/code/framework_simulator/synthetic_dags.g', 'r') as fd:
        graph_strs = fd.read().split('#')[1:]
        graphs_pool = {}
        for g_str in graph_strs: # for each graph 
            g = build_dag_from_str(g_str)
            graphs_pool[g.gp['name']] = g
        print(len(graphs_pool), "loaded successfully")
        return graphs_pool
    
ls = {0 : 'tiny', 1: 'sequential', 2: 'aggregate', 3: 'broadcast', 4: 'complex'}



if __name__ == '__main__':
    graphs_pool = build_tpc_graphpool();
     
    print('{')
    for gid in graphs_pool:
        _type =  ls[graphs_pool[gid].category]
        if 'HQ' in gid and '-' not in gid:
            print('"' + gid.replace('H', '') + '" : "' + ls[graphs_pool[gid].category] + '",')

    print('}')
#    convert_to_graphtool(graphs_pool)
#    graphs_pool = load_synthetic_graphs();

