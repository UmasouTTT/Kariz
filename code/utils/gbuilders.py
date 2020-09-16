import utils.graph as graph
import utils.job as job
import graph_tool.all as gt
import json

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
        raise NameError("Graph name %s is invalid" % (g_name))

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
    vertices = {}
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
            inputs = result.replace(":" + extra, "")
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
            if v1 == v2:  # and len(vertices) != 1:
                g.add_new_job(v1)

            g.config_inputs(v1, vertices[v1]['inputs'])

            for i in vertices[v1]['inputs']:
                if i in vertices[v2]['output']:
                    g.add_edge(v2, v1, 0)
    # print(str(g))
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
    # g.total_runtime = raw_dag['total_runtime']
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
    g_name, g_type = g_elements[0].split(',')[1:]
    g_id = 0
    g_queuetime = 0

    g = gt.Graph(directed=True)
    g.gp['name'] = g.new_graph_property("string", g_name)
    g.gp['id'] = g.new_graph_property("string", str(g_id))
    g.gp['queue_time'] = g.new_graph_property("int", g_queuetime)
    g.gp['cur_stage'] = g.new_graph_property("int", -1)
    g.gp['uuid'] = g.new_graph_property("string", '')

    status = g.new_vertex_property("int")
    inputs = g.new_vertex_property("object")
    cache_runtime = g.new_vertex_property("int")
    remote_runtime = g.new_vertex_property("int")
    ops = g.new_vertex_property("vector<string>")

    # build vertices
    for el in g_elements[1:]:
        if el.startswith('v'):
            print(el)
            vid, inputs_str, operation = el.split(',')[1:]
            v = g.add_vertex()
            inputs[v] = build_input_format(inputs_str)
            cache_runtime[v] = 0
            remote_runtime[v] = 0
            ops[v] = operation.split('|')

    # build edges
    for el in g_elements[1:]:
        if el.startswith('e'):
            print(el)
            v_src, v_dest = el.split(',')[1:]
            e = g.add_edge(v_src, v_dest)

    g.vp['inputs'] = inputs
    g.vp['remote_runtime'] = remote_runtime
    g.vp['cache_runtime'] = cache_runtime
    g.vp['status'] = status
    g.vp['ops'] = ops
    gt.graph_draw(g, vertex_text=g.vp.inputs, vertex_color=g.vp.color,
                  vertex_fill_color=g.vp.color, size=(700, 700))
    return g


def load_graph_skeleton(path):
    graph_skeletons = {}
    with open(path, 'r') as fd:
        graph_strs = fd.read().split('#')[1:]

        for g_str in graph_strs:
            g = build_graph_skeleton(g_str)
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


def serialize_synthetic_graph(g):
    ser_graph = {
            'type': 'synthetic',
            'id': g.gp.id,
            'uuid': g.gp.uuid,
            'nodes': [], 
            'edges': []}
    for v in g.vertices():
        ser_graph['nodes'].append({
            'vid': g.vp.vid[v],
            'compute_runtime': g.vp.job[v].t_compute,
            'reduction_ratio': g.vp.job[v].t_compute_ratio,
            'inputs': ':'.join(g.vp.job[v].inputs.keys())})

    for e in g.edges():
        ser_graph['edges'].append({
            'src': g.vp.vid[e.source()],
            'dst': g.vp.vid[e.target()]})
    return ser_graph


def deserialize_synthetic_graph(g_str, object_store):
    jgraph = json.loads(g_str)
    g = gt.Graph(directed=True)
    g.gp['id'] = g.new_graph_property("int", jgraph['id'])
    g.gp['uuid'] = g.new_graph_property("string", jgraph['uuid'])
    g.gp['queue_time'] = g.new_graph_property("int")
    g.gp['candidates'] = g.new_graph_property('object')
    g.gp['plans_container'] = g.new_graph_property('object')
    g.gp['stages'] = g.new_graph_property('object', {})
    g.gp['cur_stage'] = g.new_graph_property('int', -1)
    g.gp['schedule'] = g.new_graph_property('object', {})
    g.vp['compute_runtime'] = g.new_vertex_property("int")
    g.vp['reduction_ratio'] = g.new_vertex_property("float")
    g.vp['inputs'] = g.new_vertex_property("string")
    g.vp['job'] = g.new_vertex_property("object")
    g.vp['vid'] = g.new_vertex_property("int")
    g.vp['color'] = g.new_vertex_property("string")
    g.vp['status'] = g.new_vertex_property("int")
    g.vp['stage_id'] = g.new_vertex_property("int")
    g.vp['slevel'] = g.new_vertex_property("int")

    vid_to_v = {}
    for vmeta in jgraph['nodes']:
        v = g.add_vertex()
        vid_to_v[vmeta['vid']] = v
        g.vp.vid[v] = vmeta['vid']
        g.vp.job[v] = job.Job()
        g.vp.job[v].id = vmeta['vid']
        g.vp.job[v].initialize(vmeta['inputs'], vmeta['compute_runtime'], vmeta['reduction_ratio'], object_store)
        g.vp.color[v] = '#fb8072' if len(g.vp.inputs[v]) > 0 else '#bdbdbd'

    for emeta in jgraph['edges']:
        g.add_edge(vid_to_v[emeta['src']], vid_to_v[emeta['dst']]) 
    #gt.graph_draw(g, vertex_text=g.vp.inputs, vertex_color=g.vp.color,
    #        vertex_fill_color=g.vp.color, size=(700, 700), output='graph%d.png'%(g.gp.id))
    return g


def build_synthetic_dag_from_string(g_str, object_store):
    g_id = int(g_str.split('\n')[0].split(',')[1])
    g_elements = g_str.split('\n')[1:]
    g = gt.Graph(directed=True)
    g.gp['id'] = g.new_graph_property("int", g_id)
    g.gp['uuid'] = g.new_graph_property("string")
    g.gp['queue_time'] = g.new_graph_property("int", 10)
    g.gp['candidates'] = g.new_graph_property('object')
    g.gp['stages'] = g.new_graph_property('object', {})
    g.gp['schedule'] = g.new_graph_property('object', {})
    g.gp['plans_container'] = g.new_graph_property('object')
    g.gp['cur_stage'] = g.new_graph_property('int', -1)
    g.vp['compute_runtime'] = g.new_vertex_property("int")
    g.vp['reduction_ratio'] = g.new_vertex_property("float")
    g.vp['inputs'] = g.new_vertex_property("string")
    g.vp['job'] = g.new_vertex_property("object")
    g.vp['vid'] = g.new_vertex_property("int")
    g.vp['color'] = g.new_vertex_property("string")
    g.vp['status'] = g.new_vertex_property("int")
    g.vp['stage_id'] = g.new_vertex_property("int")
    g.vp['slevel'] = g.new_vertex_property("int")
    
    vid_to_v = {}
    for el in g_elements:
        if el.startswith('v'):
            vid, vin, compute_time, reduction_ratio = el.split(',')[1:]
            if vid in vid_to_v:
                raise NameError('vertex id must be unique: line: %s'%(el))
            v = g.add_vertex()
            vid_to_v[int(vid)] = v
            g.vp.vid[v] = int(vid)
            g.vp.job[v] = job.Job()
            g.vp.job[v].initialize(vin, int(compute_time), float(reduction_ratio), object_store)
            g.vp.color[v] = '#fb8072' if len(g.vp.inputs[v]) > 0 else '#bdbdbd'

    for el in g_elements:
        if el.startswith('e'):
            src, dst = el.split(',')[1:]
            g.add_edge(vid_to_v[int(src)], vid_to_v[int(dst)])
    #gt.graph_draw(g, vertex_text=g.vp.inputs, vertex_color=g.vp.color,
    #        vertex_fill_color=g.vp.color, size=(700, 700), output='graph%d.png'%(g.gp.id))
    return g


def load_synthetic_dags(fpath, object_store):
    graphs_pool = {}
    with open(fpath, 'r') as fd:
        raw_g_str = fd.read().split('#')[1:]
        for g_str in raw_g_str:
            g = build_synthetic_dag_from_string(g_str, object_store)
            graphs_pool[g.gp.id] = g
    return graphs_pool
