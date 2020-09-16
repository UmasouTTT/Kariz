#!/usr/bin/python3
#import common
import configs.tpch as cfg
#import d3n.metadata as md
import pandas as pd
import graph_tool.all as gt
import requests
import json
import os
import subprocess
import time
from colorama import Fore, Style

def get_s3_objects(jobid):
    host_name='neu-3-1'
    port=19888
    url = 'http://%s:%d/ws/v1/history/mapreduce/jobs/%s/conf'%(host_name, port, jobid)
    r=requests.get(url)
    confs = r.json()['conf']['property']
    return [i.split('/')[-1] for i in [c['value'] for c in confs if c['name'] == 'pig.input.dirs'][0].split(',') if i.startswith('s3a')]


def build_dag(dag_str, df):
    g = gt.Graph(directed=True)

    jid_to_vis = {}
    vis_to_jid = {}

    for ln in dag_str:
        vertices = []
        for vstr in ln.replace('\t', '').split(',')[:-1]:
            vertices.extend(vstr.split('->'))

#        print(vertices)
        for jid in vertices:
            if jid not in jid_to_vis:
                v = g.add_vertex()
                jid_to_vis[jid] = v
                vis_to_jid[v] = jid

    for ln in dag_str:
        vertices = ln.replace('\t', '').split('->')
        if len(vertices) == 2:
            src_jid = vertices[0]
            dst_jids = vertices[1].split(',')[:-1]
            for dst_jid in dst_jids:
                g.add_edge(jid_to_vis[src_jid], jid_to_vis[dst_jid])


    v_feature = g.new_vertex_property('string')
    v_tables = g.new_vertex_property('string')

    for v in g.vertices():
        v_feature[v] = df[df['JobId'] == vis_to_jid[v]]['Feature'].values[0].replace(',', ':')
        v_tables[v] = ':'.join(get_s3_objects(vis_to_jid[v]))

    g.vp.feature = v_feature
    g.vp.tables = v_tables

#    gt.graph_draw(g, vertex_text=g.vertex_index, size=(300, 300))
    return g


def add_dag_to_pool(g, app_name):
    g_str = '#t\t%s\n'%(app_name)
    for v in g.vertices():
        g_str += 'v,%d,%s,%s\n'%(int(v), g.vp.feature[v], g.vp.tables[v])
    for e in g.edges():
        g_str += 'e,%d,%d\n'%(int(e.source()), int(e.target()))

    templatef='pig.tpch.template'
    with open(templatef, 'a') as fd:
        fd.write(g_str)
    pass




def run_single_query(dataset, query):
    print(Fore.LIGHTYELLOW_EX, '\tStart query %s execution'%(query), Style.RESET_ALL)
    mapsize = 512*1024*1024
    cur_dir = os.getcwd()
    os.chdir(cfg.benchmark_root)
    process = subprocess.Popen([cfg.executable, dataset, cfg.output_path, '1', str(query), str(mapsize)], 
            stderr=subprocess.PIPE, stdout=subprocess.PIPE)
    stdoutdata, stderrdata = process.communicate()
    os.chdir(cur_dir)
    string = stdoutdata.decode()
    dag_str = [ln for ln in string.split('Job DAG:')[1].split('\n') if ln.startswith('job')]
    dag_runtime = [int(ln.split('\t')[1]) for ln in string.split('Job DAG:')[1].split('\n') if 'times (sec):' in ln][0]
    
    app_meta = {'app_name': 'TPCH_Q%s'%(query)}

    substring = [i for i in string.split('Success!')[1].split('Input(s):')[0].split('\n') if i][1:]
    df = pd.DataFrame([{**dict(zip(substring[0].split('\t'), s.split('\t'))), **app_meta} for s in substring[1:]])

    g = build_dag(dag_str, df)
    add_dag_to_pool(g, app_meta['app_name'])
    pass


def run_feature_extraction_benchmark():
    path = cfg.benchmark_root
    # for all queries
    queries = [f.replace('Q', '').replace('.pig', '') for f in os.listdir('%s/queries'%(cfg.benchmark_root)) if f.endswith('.pig')]
    dataset = 's3a://data/pig-tpch/20G'
    for q in queries:
        if q in ['12', '17', '9', '4', '16']:
            continue
        run_single_query(dataset, q)
    pass


#obj_store = md.ObjectStore();
#obj_store.load_metadata();
run_feature_extraction_benchmark()
