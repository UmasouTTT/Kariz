#!/usr/bin/python
import datetime

from utils.graph import *
import utils.requester as requester
import utils.status as status
import plans.scoreboard as sb
import pandas as pd
from colorama import Fore, Style


import controller.config as cfg
from controller.plans.multidag.planner import Planner


class Mirab(Planner):
    def __init__(self):
        Planner.__init__(self);

        self.fairness_factor = 0.2
        self.fairness_scores = {}
        self.share_scores = {}
        self.alpha = 0.5
        self.available_bandwidth = cfg.bandwidth  # 10Gbps = 1.2 GBps = 1200 MBps = 1200
        self.cache_block_size = 1  # 1MBype

        self.stats = []

        score_board_time = 60000 # 600 second
        self.bw_sb = sb.ScoreBoard(score_board_time, self.available_bandwidth)

    def add_dag(self, g):
        new_dp = Planner.initialize_signle_dag_planner(g);

        with self.dag_planners_mutex.writer_lock():
            self.dag_planners[g.dag_id] = new_dp
            self.fairness_scores[g.dag_id] = 1

        print(Fore.LIGHTRED_EX, "\tAdded", g.name, ', Id', g.dag_id, Fore.BLUE,
              "\t Number of outstanding DAGs:", len(self.dag_planners), Style.RESET_ALL)

    def markas_pinned_datasets(self, dag_id, plan):
        with self.dag_planners_mutex.reader_lock():
            if dag_id in self.dag_planners:
                self.dag_planners[dag_id].markas_pinned_datasets(plan)

    def unpinned_completed_stage(self, dag_id, stage_id):
        with self.dag_planners_mutex.reader_lock():
            if dag_id in self.dag_planners:
                self.dag_planners[dag_id].unpinned_completed_stage(stage_id)

    def delete_dag(self, dag_id):
        with self.dag_planners_mutex.reader_lock():
            if dag_id in self.dag_planners:
                self.dag_planners[dag_id].dump_stats()

        with self.dag_planners_mutex.writer_lock():
            del self.fairness_scores[dag_id]
            del self.dag_planners[dag_id]

    def online_planner(self, dag_id, stage_id):
        start_time = datetime.datetime.now()

        # unpin files from previous stage
        self.unpinned_completed_stage(dag_id, stage_id)

        plans = self.get_plans(dag_id, stage_id)
        if plans is None:
            return

        # while there is no cache space avaialbe or no space left in bw
        while len(plans) > 0:
            plan = plans.pop(0)
            #print(Fore.LIGHTYELLOW_EX, "Mirab, process plans of DAG", plan.dag_id,
            # ', stage', plan.stage_id, Style.RESET_ALL)
            if not plan.is_feasible():
                continue
            if plan.type == 0:
                if requester.cache_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    self.update_infeasible(plan)
                    continue
                self.markas_pinned_datasets(plan.dag_id, plan)
                with self.dag_planners_mutex.reader_lock():
                    if dag_id in self.dag_planners:
                        self.dag_planners[dag_id].update_statistics(plan.stage_id, plan.data)
            else:
                # Todo: check if there is enough bandwidth till the time this plan is required
                if self.bw_sb.check_availability(plan.size) and requester.prefetch_plan(plan) != status.SUCCESS:
                    # for all priority plans larger than this priority on this stage mark them as infeasible
                    self.update_infeasible(plan)
                    continue
                self.bw_sb.inject(plan.size)

            #print(Fore.LIGHTGREEN_EX, "\t plan ", plan.data, ' is cached/prefetched: ', plan.type, Style.RESET_ALL)
            self.update_fairness_score(plan)
            self.compute_weighted_scores(plans)

        elapsed_time = datetime.datetime.now() - start_time
        outstanding_prefetch = self.bw_sb.get_outstanding_prefetch()
        print(Fore.LIGHTGREEN_EX, "\t Number of outstanding DAGs:", len(self.dag_planners),
              "Elapsed time:", elapsed_time, Fore.GREEN, "Outstanding prefetching bytes",
              outstanding_prefetch, Style.RESET_ALL)

        self.stats.append({'runtime': elapsed_time.total_seconds(), 'n_dags': len(self.dag_planners),
                           'prefetch_MB': outstanding_prefetch})

    def update_planned_bandwidth(self, plan):
        return 0

    def get_stage_plans(self, dag_id, stage):
        return self.dags[dag_id].plans_container.get_stage_plans(stage)

    def get_plans_bystage(self, dag_id, stage):
        return None if dag_id not in self.dags else self.dags[dag_id].plans_container.get_stage_plans_bypriority(stage)

    def get_plans(self, dag_id, stage):
        plans = []
        with self.dag_planners_mutex.reader_lock():
            if len(self.dag_planners) == 0:
                return plans
            available_bw = self.available_bandwidth / len(self.dag_planners)  # give every Job a fair share of DAG
            for gid in self.dag_planners:
                plans.extend(self.dag_planners[gid].get_next_plans(available_bw))
            if dag_id in self.dag_planners:
                self.dag_planners[dag_id].current_running_stage = stage
        self.compute_share_scores(plans)
        return plans

    def compute_share_scores(self, plans):
        """
        :This is O(number of plans * number of files)
        :param plans:
        :return:
        """
        files = {}
        for p in plans:
            for ds in p.data:
                size = p.data[ds]['size']
                if ds not in files:
                    files[ds] = {'dags': {}, 'n_dags': 0, 'min': -1, 'max': -1}

                if p.dag_id not in files[ds]['dags']:
                    files[ds]['dags'][p.dag_id] = []
                    files[ds]['n_dags'] += 1

                files[ds]['dags'][p.dag_id].append(size)

                if files[ds]['min'] == -1 or size < files[ds]['min']:
                    files[ds]['min'] = size

        # look at the eqaution 4 in Eurosys submission in the paper
        for p in plans:
            w_p = 0
            for ds in p.data:
                if files[ds]['n_dags'] > 1:
                    w_p += (1/len(files[ds]['n_dags']))
            p.sscore = 1 - w_p/p.size


    def compute_weighted_scores(self, plans):
        for p in plans:
            with self.dag_planners_mutex.reader_lock():
                if p.dag_id in self.fairness_scores:
                    p.wscore = self.alpha * self.fairness_scores[p.dag_id] + (1 - self.alpha) * p.sscore
        plans.sort(reverse=True)

    def updateby_share_score(self, plans):
        share_score = 0
        input_count = 1
        return share_score / input_count

    def updateby_pscore(self, plans):
        share_score = 0
        input_count = 1
        return share_score / input_count

    def update_fairness_score(self, plan):
        with self.dag_planners_mutex.reader_lock():
            for gid in self.dag_planners:
                sign = -1 if gid == plan.dag_id else +1
                self.fairness_scores[gid] = self.fairness_scores[gid] + sign * self.fairness_scores[
                    gid] * self.fairness_factor + sum(self.fairness_scores.values()) / len(self.fairness_scores)

    def update_infeasible(self, plan):
        plan.feasible = 0
        # FIXME this should the cache

    def end_of_experiment_alert(self):
        df = pd.DataFrame(self.stats)
        with open("scalability_test.csv", 'a') as fd:
            df.to_csv(fd, index=False, header=False)

        # reset score board
        self.bw_sb.reset()
