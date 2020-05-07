#!/usr/bin/python3

import dataset as ds
import randomize as rnd 

class Task:
    def __init__(self):
        self.n_inputs = 0;
        self.inputs = {};
        self.total_input_sz = 0
        self.fetch_time = 0;
        
        self.n_map = 0;
        self.map_runtime = 0;
        
        self.n_reduce = 0;
        self.reduce_runtime = 0;
        
        self.total_runtime = self.fetch_time + self.map_runtime + self.reduce_runtime
        
    
    def __str__(self):
        return 'runtime: ' + str(self.total_runtime) + \
               ' --> [' + str(self.fetch_time) + ', ' + str(self.map_runtime) + \
               ', ' + str(self.map_runtime) + ']\n' + \
               'inputs: ' + str(self.total_input_sz) + \
               ' --> [' + str(self.inputs[1].name) + ': ' + str(self.inputs[1].size) + ']\n'
    
    def random_runtime(self):
        self.fetch_time = 0;
        
        self.n_map = 0;
        self.map_runtime = rnd.random_runtime();
        
        self.n_reduce = 0;
        self.reduce_runtime = rnd.random_runtime();
        
        self.total_runtime = self.fetch_time + self.map_runtime + self.reduce_runtime

        
    def random_input(self):
        # number of inputs
        self.n_inputs = 1;
        for i in range(0, self.n_inputs):
            in_file = rnd.random_file_id();
            self.inputs[in_file] = ds.Object(in_file, ds.datalake[in_file]);
            self.total_input_sz += self.inputs[in_file].size; 
            
    ''' 
    input: bandwidth represented in Gbps
    retun: time in sec
    '''
    def estimate_runtime(self, bandwidth=1):
        if bandwidth <= 0: 
            raise NameError('Bandwidth <= 0!')
        
        self.fetch_time = self.total_input_sz/((10**9)*bandwidth);
        return self.fetch_time + self.map_runtime + self.reduce_runtime;

