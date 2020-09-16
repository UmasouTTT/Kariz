#!/usr/bin/python3
# Mania Abdi

from graph_tool.all import *

i = 0
def draw_dags(dag_str):
    global i;
    i = i + 1;
    g = Graph(directed=True)
    v_map = {}

    for strs in dag_str:
        if '->' in strs:
            src_vtx = strs.split("->")[0].replace("\n", '').split('scope')[0]
            dest_vtx = strs.split("->")[1].replace("\n", '').split('scope')[0]
            
            if src_vtx not in v_map:
                v_src = g.add_vertex()
                v_map[src_vtx] = v_src

            if dest_vtx not in v_map:
                v_dest = g.add_vertex()
                v_map[dest_vtx] = v_dest
            
            g.add_edge(v_src, v_dest)

    graph_draw(g, vertex_text=g.vertex_index, vertex_font_size=18,
            output_size=(200, 200), output="two-nodes" + str(i) + ".png")

#read files
with open('tpch_pig_tez.txt', 'r') as fd:
    dag_structure = []
    capture = False
    for line in fd:
        if line.startswith("DAG Plan:"):
            capture = True
        elif line.startswith("Vertex Stats:"):
            capture = False
            draw_dags(dag_structure)
            dag_structure = []
        elif capture:
            dag_structure.append(line)
