#!/usr/bin/python
import datetime

from utils.graph import *
import utils.requester as requester
import utils.status as status
#import plans.scoreboard as sb
import pandas as pd
from colorama import Fore, Style


import controller.config as cfg
from controller.plans.multidag.planner import Planner

print_friendy = {0: 'cached', 1: 'prefetched'}

class Mirab(Planner):
    def __init__(self, object_store):
        Planner.__init__(self);
        self.fairness_factor = 0.2
        self.fairness_scores = {}
        self.share_scores = {}
        self.alpha = 0.5
        self.cache_block_size = 1  # 1MBype
        self.stats = []
        self.object_store = {} #self.build_object_store(object_store)
        self.all_plan = {}
        score_board_time = 60000 # 600 second
        #self.bw_sb = sb.ScoreBoard(score_board_time, self.available_bandwidth)
        pass


    def build_object_store(self, object_store):
        blocks = {}
        for f in object_store:
            size_f = object_store[f]
            for i in range(0, size_f):
                blocks[(f, i)] = {'dags': {}, 'n_dags': 0}
        return blocks

    def reset_object_store(self):
        for block in self.object_store:
            self.object_store[block] = {'dags': {}, 'n_dags': 0}
        pass

    def add_dag(self, g):
        print('G is', g)
        new_dp = Planner.initialize_signle_dag_planner(self, g);

        with self.dag_planners_mutex.writer_lock():
            self.dag_planners[g.gp.id] = new_dp
            self.all_plan.update(g.gp.plans_container.pc_by_uuid)
            #self.fairness_scores[g.gp.id] = 1

        print(Fore.LIGHTRED_EX, "\tAdded", g.gp.uuid, ', Id', g.gp.id, Fore.BLUE,
              "\t Number of outstanding DAGs:", len(self.dag_planners), Style.RESET_ALL)
        #self.online_planner(g.gp.id, g.gp.cur_stage)
        return

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
            #del self.fairness_scores[dag_id]
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
            self.prefech_or_cache_candidate(plan, dag_id, stage_id)
            for cp_uuid in plan.corrolated:
                self.all_plan[cp_uuid].type = 1
                print(Fore.LIGHTYELLOW_EX, 'plan data', plan.data, "corrolated plans", self.all_plan[cp_uuid].data, Style.RESET_ALL)
                self.prefech_or_cache_candidate(self.all_plan[cp_uuid], dag_id, stage_id)

        elapsed_time = datetime.datetime.now() - start_time
        outstanding_prefetch = 0#self.bw_sb.get_outstanding_prefetch()
        print(Fore.LIGHTGREEN_EX, "\t Number of outstanding DAGs:", len(self.dag_planners),
              "Elapsed time:", elapsed_time, Fore.GREEN, "Outstanding prefetching bytes",
              outstanding_prefetch, Style.RESET_ALL)

        self.stats.append({'runtime': elapsed_time.total_seconds(), 'n_dags': len(self.dag_planners),
                           'prefetch_MB': outstanding_prefetch})

    def prefech_or_cache_candidate(self, plan, dag_id, stage_id):
        if not plan.is_feasible(): return

        if plan.type == 0:
            if requester.cache_plan(plan) != status.SUCCESS:
                # for all priority plans larger than this priority on this stage mark them as infeasible
                print(Fore.LIGHTRED_EX, "\t plan ", plan.data, ' is not ', print_friendy[plan.type],
                      'plan score is', plan.sscore, Style.RESET_ALL)
                plan.update_infeasible()
                return
            self.markas_pinned_datasets(plan.dag_id, plan)
            plan.update_status()
            print(Fore.LIGHTBLUE_EX, "\t plan ", plan.data, ' is ',
                  print_friendy[plan.type], 'plan score is', plan.sscore, 'status', plan.status, Style.RESET_ALL)
            with self.dag_planners_mutex.reader_lock():
                if dag_id in self.dag_planners:
                    self.dag_planners[dag_id].update_statistics(plan.stage_id, plan.data)
        else:
            # Todo: check if there is enough bandwidth till the time this plan is required
            if self.all_plan[plan.uuid].status == 0:
                # if self.bw_sb.check_availability(plan.size) and requester.prefetch_plan(plan) != status.SUCCESS:
                if requester.prefetch_plan(plan) != status.SUCCESS:
                    # mark all candidates who inherited from this one as infeasible
                    print(Fore.LIGHTRED_EX, "\t plan ", plan.data, ' is not ', print_friendy[plan.type],
                          'plan score is', plan.sscore, Style.RESET_ALL)
                    plan.update_infeasible()
                    return
                # self.bw_sb.inject(plan.size)
                print(Fore.LIGHTGREEN_EX, "\t plan ", plan.data, ' is ',
                      print_friendy[plan.type], 'plan score is', plan.sscore, 'status', plan.status, Style.RESET_ALL)
                for f in plan.data:
                    self.prefetch_count += plan.data[f]['size']
                self.all_plan[plan.uuid].status = 1
                plan.update_status()
            plan.type = 0

        pass


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
            available_bw = self.remote_bw / len(self.dag_planners)  # give every Job a fair share of DAG
            for gid in self.dag_planners:
                plans.extend(self.dag_planners[gid].get_next_plans(available_bw))
            if dag_id in self.dag_planners:
                self.dag_planners[dag_id].current_running_stage = stage
        self.compute_share_scores(plans)
        return plans

    def compute_share_scores(self, plans):
        """
        :This is O(number of plans * number of blocks in a file)
        :param plans:
        :return:
        """
        for p in plans:
            for ds in p.data:
                for block in range(0, p.data[ds]['size']):
                    if p.dag_id not in self.object_store[(ds, block)]['dags']:
                        self.object_store[(ds, block)]['dags'][p.dag_id] = []
                        self.object_store[(ds, block)]['n_dags'] += 1
                    self.object_store[(ds, block)]['dags'][p.dag_id].append(p.uuid)

        for p in plans:
            for ds in p.data:
                for block in range(0, p.data[ds]['size']):
                    p.unique_size += 1/self.object_store[(ds, block)]['n_dags']

                    for gid in self.object_store[(ds, block)]['dags']:
                        if gid != p.dag_id:
                            for puid in self.object_store[(ds, block)]['dags'][gid]:
                                if puid not in p.corrolated:
                                    p.corrolated[puid] = 1
        for p in plans:
            p.sscore = p.iscore/p.unique_size
        plans.sort(reverse=True)
        return


    def compute_weighted_scores(self, plans):
        for p in plans:
            with self.dag_planners_mutex.reader_lock():
                if p.dag_id in self.fairness_scores:
                    p.wscore = self.alpha * self.fairness_scores[p.dag_id] + (1 - self.alpha) * p.sscore


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

    #def end_of_experiment_alert(self):
     #   print("---------------------------->>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>", self.prefetch_count)
        #df = pd.DataFrame(self.stats)
        #with open("scalability_test.csv", 'a') as fd:
        #    df.to_csv(fd, index=False, header=False)

        # reset score board
        #self.bw_sb.reset()
