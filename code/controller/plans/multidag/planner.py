#!/usr/bin/python
import datetime
import controller.config as cfg
import d3n.metadata as md
from prwlock import RWLock



class Planner:
    def __init__(self):
        self.object_store = md.ObjectStore();
        self.object_store.load_metadata();
        self.dag_planners = {}
        self.remote_bw = cfg.remote_bandwidth  # 10Gbps = 1.2 GBps = 1200 MBps = 1200
        self.cache_bw = cfg.cache_bandwidth  # 10Gbps = 1.2 GBps = 1200 MBps = 1200
        self.dag_planners_mutex = RWLock()


    def initialize_signle_dag_planner(self, g):
        if cfg.single_dag_replacement == 'cmr': # 
            import controller.plans.singledag.cmr as sdag
            return sdag.CMR(g, self.remote_bw, self.cache_bw)
        elif cfg.single_dag_replacement == 'mrd': # minimum reference distance 
            import controller.plans.singledag.mrd as sdag
            return sdag.MRD(g)
        elif cfg.single_dag_replacement == 'cp': # crtitical path
            import controller.plans.singledag.cp as sdag
            return sdag.CP(g)
        elif cfg.single_dag_replacement == 'rcp': # recursive critical path
            import controller.plans.singledag.rcp as sdag
            return sdag.RCP(g)
        elif cfg.single_dag_replacement == 'prcp': # partial recursive critical path
            import controller.plans.singledag.prcp as sdag
            return sdag.prcp(g)
        elif cfg.single_dag_replacement == 'nocache': # partial recursive critical path
            import controller.plans.singledag.nocache as sdag
            return sdag.NoCache(g)
        raise NameError("Please specify single DAG planner")


    def add_dag(self, g):
        pass

    def markas_pinned_datasets(self, dag_id, plan):
        pass

    def unpinned_completed_stage(self, dag_id, stage_id):
        pass

    def delete_dag(self, dag_id):
        pass

    def online_planner(self, dag_id, stage_id):
        pass

    def update_planned_bandwidth(self, plan):
        pass

    def get_stage_plans(self, dag_id, stage):
        pass

    def get_plans_bystage(self, dag_id, stage):
        pass

    def get_plans(self, dag_id, stage):
        pass
        # FIXME this should the cache

    def end_of_experiment_alert(self):
        pass
