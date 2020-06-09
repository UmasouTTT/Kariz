import random
import pigsimulator as pigsim
import threading
import tpc
from colorama import Fore, Style
import workload.config as cfg
from threading import Thread
import uuid
import utils.requester as req


class Workload:
    def __init__(self):
        self.n_intervals = cfg.simulation_period//cfg.submission_interval
        self.dags_pool = tpc.load_synthetic_graphs()
        self.pendings = []

    def load_fixed_workload(self):
        with open(cfg.fixed_workload_path, 'r') as fd:
            return fd.read().split(',')

    def create_fixed_workload(self):
        with open(cfg.fixed_workload_path, 'w') as fd:
            fd.write(','.join(random.choices(list(self.dags.keys()), k=cfg.fixed_workload_count)))

    def submit_dag(self, dag_name):
        dags = self.dags_in_ex

        for dag_name in dags:
            t = Thread(target=self.submit_dag, args=(dag_name,))
            t.start()
            self.pendings.append(t)

    def run(self):
        elapsed_time = 0;

        self.load_fixed_workload();

        dags_in_ex = self.load_fixed_workload();
        for g_name in dags_in_ex:


        # initialize a timer that issues submit DAG every two seconds
        ticker = threading.Event()
        while elapsed_time < cfg.simulation_period:
            ticker.wait(cfg.submission_interval)

            self.select_and_submit(cfg.n_dags_per_interval)

            elapsed_time += cfg.submission_interval;
            print(Fore.LIGHTRED_EX, 'Number of pending tasks', len(self.pendings), Style.RESET_ALL)

            if cfg.type == 'sequential':
                # wait for the current DAGs to finish
                for t in self.pendings:
                    t.join()

        # join all DAGs to finish
        for t in self.pendings:
            t.join()
