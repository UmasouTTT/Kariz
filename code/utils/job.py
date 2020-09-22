from collections import defaultdict
import random
import sched, threading, time
import utils.randoms
import uuid
import ast
import math

class Job:
    def __init__(self, id=-1):
        """ Initialize a MapReduce job/Spark stage container"""
        self.id = id  # index of job
        self.inputs = {} # inputs to job
        self.num_task = 1  # number of mappers
        self.op = 'NOP'

        """Runtime related """
        self.runtime_cache = 0
        self.runtime_remote = 0
        self.runtime_partial_cached = 0
        self.t_compute = 0
        self.t_compute_ratio = 0


        """Graph related members"""
        self.slevel = 0
        self.bfs_level = 0

    """
    Compare the runtime of two jobs with each other
    This is used when I sort jobs
    within a stage according to their runtime
    """
    """
    def __lt__(self, other):
        return (self.id != other.id) and (self.runtime_partial_cached < other.runtime_partial_cached)

    def __eq__(self, other):
        return (self.id == other.id) and (self.runtime_partial_cached == other.runtime_partial_cached)

    def __gt__(self, other):
        return (self.id != other.id) and (self.runtime_partial_cached > other.runtime_partial_cached)

    def __le__(self, other):
        return (self.id != other.id) and (self.runtime_partial_cached <= other.runtime_partial_cached)

    def __ge__(self, other):
        return (self.id != other.id) and (self.runtime_partial_cached >= other.runtime_partial_cached)

    def __ne__(self, other):
        return (self.id != other.id) and (other.runtime_partial_cached != self.runtime_partial_cached)
    """
    def __str__(self):
        jobstr = '{"id" : ' + str(self.id)
        jobstr += ', "runtime_cache":' + str(self.runtime_cache)
        jobstr += ', "runtime_remote":' + str(self.runtime_remote)
        jobstr += ', "remote_misestimation":' + str(self.remote_misestimation)
        jobstr += ', "cache_misestimation":' + str(self.cache_misestimation)
        jobstr += ', "num_task":' + str(self.num_task)
        jobstr += ', "children":' + str(self.children) + ', "inputs":' + str(self.inputs) + '}'
        return jobstr

    def reset(self):
        self.runtime_partial_cached = self.runtime_remote
        self.bfs_level = 0
        self.blevel = 0
        self.tlevel = 0
        self.slevel = 0

    def to_str(self):
        jobstr = '{"id" : ' + str(self.id)
        jobstr += ', "runtime_cache":' + str(self.runtime_cache)
        jobstr += ', "runtime_remote":' + str(self.runtime_remote)
        jobstr += ', "remote_misestimation":' + str(self.remote_misestimation)
        jobstr += ', "cache_misestimation":' + str(self.cache_misestimation)
        jobstr += ', "num_task":' + str(self.num_task)
        jobstr += ', "children":' + str(self.children) + ', "inputs":' + str(self.inputs) + '}'
        return jobstr

    def set_misestimation(self, rmse, cmse):
        self.remote_misestimation = rmse
        self.cache_misestimation = cmse;

    def config_misestimated_runtimes(self, mse_factor):
        if self.remote_misestimation:
            self.est_runtime_remote += self.est_runtime_remote * mse_factor
            self.runtime_partial_cached = self.est_runtime_remote
        if self.cache_misestimation:
            self.est_runtime_cache += self.est_runtime_cache * mse_factor

    def longer_than(self, other):
        return self.runtime_partial_cached > other.runtime_partial_cached

    def concurrent_with(self, other):
        return self.runtime_partial_cached == other.runtime_partial_cached

    def longer_than_wcache(self, other):
        return self.runtime_cache > other.runtime_cache

    def random_runtime(self, _min=1, _max=10):
        self.est_runtime_remote = self.runtime_remote = random.randint(_min, _max)
        self.est_runtime_cache = self.runtime_cache = random.randint(_min, self.est_runtime_remote)
        self.runtime_partial_cached = self.est_runtime_remote
        self.final_runtime = self.est_runtime_remote

    def config_operation(self, op):
        self.op = op

    def static_runtime(self, runtime_remote, runtime_cache):
        self.est_runtime_remote = self.runtime_remote = runtime_remote
        self.est_runtime_cache = self.runtime_cache = runtime_cache
        self.runtime_partial_cached = self.est_runtime_remote
        self.final_runtime = self.est_runtime_remote

    def estimated_runtimes(self, runtime_remote, runtime_cache):
        self.runtime_partial_cached = self.est_runtime_remote = runtime_remote
        self.est_runtime_cache = runtime_cache

    def config_ntasks(self, n_tasks):
        self.num_task = n_tasks

    def config_inputs(self, inputs):
        self.inputs = inputs

    def initialize(self, inputs_str, t_compute, compute_ratio, object_store):
        for iobj in ' '.join(inputs_str.split(':')).split() :
            self.inputs[iobj] = object_store[iobj]
        self.t_compute = t_compute
        self.t_compute_ratio = compute_ratio

    def initialize_tpch(self, inputs_str, feature, t_compute, compute_ratio, object_store):
        for iobj in ' '.join(inputs_str.split(':')).split() :
            self.inputs[iobj] = object_store[iobj]
        self.op = feature
        self.t_compute = t_compute
        self.t_compute_ratio = compute_ratio


    def predict_runtime(self, remote_bw, cache_bw):
        self.runtime_remote = math.ceil(8*sum(self.inputs.values()) / remote_bw) + self.t_compute
        self.runtime_cache = math.ceil(8*sum(self.inputs.values()) / cache_bw) + self.t_compute*self.t_compute_ratio
        self.runtime_partial_cached = self.runtime_remote
        pass
