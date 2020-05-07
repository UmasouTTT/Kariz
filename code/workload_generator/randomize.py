#!/usr/bin/python3

import numpy as np
import matplotlib.pyplot as plt


def random_runtime():
    norm_mu, norm_std = 80, 300;
    return np.floor(np.absolute(np.random.normal(norm_mu, norm_std, 1)))[0]

def random_file_id():
    zipf_param = 1.3 # parameter
    cut = 1000
    return 1;
    #return np.random.zipf(a, 1)[0]%1000


