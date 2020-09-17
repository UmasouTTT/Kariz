#!/usr/bin/python
# Author: Trevor Nogues, Mania Abdi

#!/usr/bin/python
import utils.scheduler as sched
import utils.requester as req
import utils.gbuilders as gbuilder
import utils.pig as pig
import time
import scheduler.bsp as bsp

# write a class to build schedule dag in pig
NOCACHE = 0
KARIZ = 1
MRD = 2
CP=3
RCP=4
LRU=5
INFINITE=6


def start_pig_simulator(g):
    cache = NOCACHE
    pig.build_stages(g);

    req.send_new_dag_rpc(gbuilder.serialize_synthetic_graph(g))
    bsp.assign_stages(g)
    req.send_stage_start_rpc(req.serialize_stage(g))

    time.sleep(g.gp.queue_time)
    stats = bsp.execute_dag(g)

    req.send_dag_completion_rpc(req.serialize_dag_complete(g))
    return stats
    # build cache plans
    '''
    req.submit_new_dag(g)
    req.notify_stage_start(g, -1)
    if cache == KARIZ:
        g.plans_container = pig.build_kariz_priorities(g)
    elif cache == MRD:
        g.plans_container = pig.build_mrd_priorities(g)
    elif cache == CP:
        g.plans_container = pig.build_cp_priorities(g)
    elif cache == RCP:
        g.plans_container = pig.build_rcp_priorities(g)
    elif cache == INFINITE:
        g.plans_container = pig.build_infinite_priorities(g)
    elif cache == LRU:
        g.plans_container = pig.build_lru_priorities(g)
    else: 
        g.plans_container = None
    
    #return sched.gang_scheduler(g)
    '''

'''
# submit one dag to pig
v = tests.graphs[1]
v.inputs = {0:['a'], 1:['d'], 2:['c'], 3:['b'], 4:['b'], 5:['e'], 6:['a'] }
v.inputSize = {0 : [8], 1:[10], 2:[9], 3:[12], 4:[12], 5:[14], 6:[8] }
v.outputSize = {0 : [1], 1:[1], 2:[1], 3:[1], 4:[1], 5:[1], 6:[1]}
'''
