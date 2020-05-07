#!/usr/bin/python3
import scheduler
import sys

def build_dag_from_str(g_str):
    g_elements=g_str.split('\n')[:-1]
    g = gt.Graph(directed=True)
    status = g.new_vertex_property("int")
    ops = g.new_vertex_property("string")
    inputs = g.new_vertex_property("string")
    outputs = g.new_vertex_property("string")

    for g_e in g_elements:
        command, params = g_e.split(',', 1)

        if command == 'v':
            vid,operation, inputdir, outdir = params.split(',')
            v = g.add_vertex()
            status[v] = 0
            ops[v] = operation
            inputs[v] = inputdir
            outputs[v] = outdir
        elif command == 'e':
            src, dest = params.split(',')
            g.add_edge(src, dest)
    g.vp['status'] = status;
    g.vp['ops'] = ops;
    g.vp['inputdir'] = inputs
    g.vp['outputdir'] = outputs
    return g;

def emulate_pig_execution(g):
    scheduler.execute_dag(g)


input_file = sys.argv[1]
with open(input_file, 'r') as fd:
    graph_strs = fd.read().split('#')[1:]

    for g_str in graph_strs:
        g = build_dag_from_str(g_str)
        emulate_pig_execution(g)
        break;
