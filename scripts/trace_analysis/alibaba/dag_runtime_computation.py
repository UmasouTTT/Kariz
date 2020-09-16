import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json


# draw runtime of tasks
batch_tasks = pd.read_csv("/local0/batch_task.csv", names=['Name', '# Inst', 'Job', 'Type', 'Status', 'Start Time', 'End Time', 'CPU', 'Mem'])
batch_tasks.dropna()

batch_tasks = batch_tasks[batch_tasks['End Time'] != 0]

batch_tasks["run time"] = pd.Series(batch_tasks['End Time'] - batch_tasks['Start Time']).abs();

print(min(batch_tasks["run time"]))
print(max(batch_tasks["run time"]))


run_time_his_dist = {
    '30 sec' : len(batch_tasks[batch_tasks["run time"] < 30]),
    '1 min' : len(batch_tasks[(batch_tasks["run time"] >= 30) & (batch_tasks["run time"] < 60)]), 
    '5 min' : len(batch_tasks[(batch_tasks["run time"] >= 60) & (batch_tasks["run time"] < 300)]), 
    '30 min' : len(batch_tasks[(batch_tasks["run time"] >= 300) & (batch_tasks["run time"] < 1800)]), 
    '1 hour' : len(batch_tasks[(batch_tasks["run time"] >= 1800) & (batch_tasks["run time"] < 3600)]),
    '< 1 day ' : len(batch_tasks[(batch_tasks["run time"] >= 3600) & (batch_tasks["run time"] < 86400)]),
    '> 1 day' : len(batch_tasks[batch_tasks["run time"] >= 86400]) 
} 

run_time_labels = ('< 30 sec', '< 1 min', '< 5 min', '< 30 min', 
                   '< 1 hour', '< 1 day', '> 1 day')


run_time_list = [
    len(batch_tasks[batch_tasks["run time"] < 30]),
    len(batch_tasks[(batch_tasks["run time"] >= 30) & (batch_tasks["run time"] < 60)]), 
    len(batch_tasks[(batch_tasks["run time"] >= 60) & (batch_tasks["run time"] < 300)]), 
    len(batch_tasks[(batch_tasks["run time"] >= 300) & (batch_tasks["run time"] < 1800)]), 
    len(batch_tasks[(batch_tasks["run time"] >= 1800) & (batch_tasks["run time"] < 3600)]),
    len(batch_tasks[(batch_tasks["run time"] >= 3600) & (batch_tasks["run time"] < 86400)]),
    len(batch_tasks[batch_tasks["run time"] >= 86400]) 
] 
y_pos = np.arange(len(run_time_labels))

run_time_list

total_number_of_jobs = sum(run_time_list)
run_time_list = [100*x / total_number_of_jobs for x in run_time_list]
plt.bar(y_pos, run_time_list, align='center', color='blue', alpha=0.5)
plt.xticks(y_pos, run_time_labels)
plt.ylabel('Percentage of jobs')
plt.xlabel('Job runtime', )
xlocs, xlabs = plt.xticks()

for i, v in enumerate(run_time_list):
    plt.text(xlocs[i] - 0.25, v + 0.01, str(round(v, 3)))
