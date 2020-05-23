#!/usr/bin/python3
import sys
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import json
import seaborn as sns;

def format_func(value, tick_number):
    return int(round(value/3600))

np.random.seed(0)
sns.set()

input_file = "/local0/batch_task.csv"
#input_file = "./batch_task.csv"
#input_file = "./10kbatches.csv"

# Read Batch Data, hint: Job and Type column should swap
data = pd.read_csv(input_file, names=['Name', '# Instances', 'Job', 'Type', 'Status', 'Start Time', 'End Time', 'CPU', 'Mem'])

# Filter jobs with dependency, dataset has two types of batch jobs, w/wo DAG info and 
data_dag = data.loc[~data['Name'].str.startswith('task_', na=False)]

# group jobs by their start time
job_groups_by_timestamp = data_dag.groupby(['Start Time'], as_index=True);
print("# of timestamps: " + str(len(job_groups_by_timestamp)))

# at each timestamp count # of submitted DAGS
dag_submitted_per_timestamp = job_groups_by_timestamp['Job'].nunique();
print("\t # of DAG submitted per timestamp:" + str(dag_submitted_per_timestamp.values))

# at each timestamp count # of submitted tasks
task_submitted_per_timestamp = job_groups_by_timestamp.size();
print("\t # of DAG submitted per timestamp:" + str(task_submitted_per_timestamp.values))

ser1 = pd.Series(index = job_groups_by_timestamp.groups, data=dag_submitted_per_timestamp)
ser1 = ser1.drop(ser1.index[ser1.idxmax()])
ser1 = ser1.drop(ser1.index[ser1.idxmax()])
cdf1 = ser1.value_counts().sort_index().cumsum()
cdf1 = cdf1/max(cdf1)

ser2 = pd.Series(index = job_groups_by_timestamp.groups, data=task_submitted_per_timestamp)
ser2 = ser2.drop(ser2.index[ser2.idxmax()])
ser2 = ser2.drop(ser2.index[ser2.idxmax()])
cdf2 = ser2.value_counts().sort_index().cumsum()
cdf2 = cdf2/max(cdf2)

df1 = ser1.to_frame()
df1.reset_index(inplace=True)
df1.columns = ['timestamp','dag_count']
with plt.style.context("seaborn-white"):
    plt.rcParams["axes.grid"] = False
    fig, ax = plt.subplots()
    df1.plot(kind='scatter', ax=ax, x='timestamp', y='dag_count', color='blue')
    ax.xaxis.set_major_formatter(plt.FuncFormatter(format_func))
    ax.set_facecolor('white')

plt.xlabel('Time (hour)', fontsize=16)
plt.ylabel('# of DAGs submitted', fontsize=16)
plt.rcParams["axes.edgecolor"] = "black"
plt.rcParams["axes.linewidth"] = 1
plt.tight_layout()


def smooth(y, box_pts):
    box = np.ones(box_pts)/box_pts
    y_smooth = np.convolve(y, box, mode='same')
    return y_smooth

print(df1)

x_data = [i+1 for i in range(len(df1['dag_count'].values))]
data = smooth(df1['dag_count'].values, 12)

count = 500000
fig, ax = plt.subplots()
ax.scatter(x_data[0:count], df1['dag_count'].values[0:count], linewidth=1, color = 'b')
data = smooth(df1['dag_count'].values[0:count], 1500)

def format_func(value, tick_number):
    return int(round(value/3600) + 1)

count = 500000
fig, ax = plt.subplots()
ax.scatter(x_data, df1['dag_count'].values, linewidth=1, color = '#bababa')
data = smooth(df1['dag_count'].values, 1500)
ax.plot(x_data, data, linewidth=1, color = '#ca0020')


ax.xaxis.set_major_formatter(plt.FuncFormatter(format_func))
ax.set_facecolor('white')

plt.xlabel('Time (hour)', fontsize=16)
plt.ylabel('# of DAGs submitted', fontsize=16)
#plt.rcParams["axes.edgecolor"] = "black"
#plt.rcParams["axes.linewidth"] = 1
plt.tight_layout()
plt.savefig('dags_submitted_per_ts_timeseries.pdf', format='pdf', dpi=200)
plt.savefig('dags_submitted_per_ts_timeseries.png', format='png', dpi=200)



