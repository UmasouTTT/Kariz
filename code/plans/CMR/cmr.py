'''
Created on May 18, 2020

@author: mania
'''
'''
Algorithm:
   - For each stage compute the plans. 
   - Sort all of the plans based on their knapsack order. 
   - For each plan compute the latest and the earliest time that prefetching should start. 
   - At the begining of each stage, for plans which earliest or latest time is in this stage are in the plan
   select those based on their knapsack and fill in the score board. Fill in score board for this stage if we 
   could 
'''
# This class should move to CMR.py

import json
import dagplanner as DP
import framework_simulator.tpc as tpc        
        
stages = {0: {'remote': 350, 'cache': 250},
          1: {'remote': 300, 'cache': 200},
          2: {'remote': 400, 'cache': 200},
          3: {'remote': 250, 'cache': 200},
          4: {'remote': 30, 'cache': 30},
          5: {'remote': 50, 'cache': 50},}



graphs_pool = tpc.build_tpc_graphpool()

DP.CMRPlanner(graphs_pool['AQ28'])
print(len(graphs_pool))
#sb = DP.CMRScoreBoard(stages, 1)