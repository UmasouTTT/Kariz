#!/usr/bin/python3
import pandas as pd
import numpy as np
import sklearn as sk
import psutil
import utils.hadoop as hadoop
import json
import datetime
import time

import estimator.config as cfg

class Predictor:
    def __init__(self):
        self.name = 'Predictor class'
        self.nic_bandwidth = psutil.net_if_stats()[cfg.nic][2]/8
        print('Total bandwidth', self.nic_bandwidth, 'MBps')
        self.stats = None


    def predict_bandwidth(self):
        t1 = datetime.datetime.now()
        nic_status_t1 = psutil.net_io_counters(pernic=True)[cfg.nic]
        elapsed = (datetime.datetime.now()-t1).total_seconds()
        nic_status_t2 = psutil.net_io_counters(pernic=True)[cfg.nic]
        nic_usage = ((nic_status_t2[1] - nic_status_t1[1])/elapsed, 2)



        

    def predit_runtime(self, conf):
        print(conf)
        
        
pred = Predictor()
pred.predict_bandwidth()

