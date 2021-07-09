#!/usr/bin/python
import datetime

from utils.graph import *
import utils.requester as requester
import utils.status as status
#import plans.scoreboard as sb
import pandas as pd
from colorama import Fore, Style


from controller.plans.multidag.planner import Planner

print_friendy = {0: 'cached', 1: 'prefetched'}

class Optimal(Planner):
    def __init__(self, object_store):
        Planner.__init__(self);
        self.cache_block_size = 1  # 1MBype
        self.stats = []
        self.object_store = self.build_object_store(object_store)
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
        new_dp = Planner.initialize_signle_dag_planner(self, g)
        with self.dag_planners_mutex.writer_lock():
            self.dag_planners[g.gp.id] = new_dp
            self.all_plan.update(g.gp.plans_container.pc_by_uuid)
        print(Fore.LIGHTRED_EX, "\tAdded", g.gp.uuid, ', Id', g.gp.id, Fore.BLUE,
              "\t Number of outstanding DAGs:", len(self.dag_planners), Style.RESET_ALL)
        return

    def online_planner(self, dag_id, stage_id):
        start_time = datetime.datetime.now()
        self.unpinned_completed_stage(dag_id, stage_id) # unpin files from previous stage
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
                self.all_plan[plan.uuid].status = 1
                plan.update_status()
            plan.type = 0
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
            del self.dag_planners[dag_id]

    def update_planned_bandwidth(self, plan):
        return 0

    def get_plans(self, dag_id, stage):
        plans = []
        with self.dag_planners_mutex.reader_lock():
            if not len(self.dag_planners) == 0: return plans
            available_bw = self.remote_bw / len(self.dag_planners)  # give every Job a fair share of DAG
            for gid in self.dag_planners:
                plans.extend(self.dag_planners[gid].get_next_plans(available_bw))
            if dag_id in self.dag_planners:
                self.dag_planners[dag_id].current_running_stage = stage
        return plans


    def end_of_experiment_alert(self):
        df = pd.DataFrame(self.stats)
        with open("scalability_test.csv", 'a') as fd:
            df.to_csv(fd, index=False, header=False)
        # reset score board
        #self.bw_sb.reset()
        pass
