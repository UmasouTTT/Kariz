#!/usr/bin/python3
import subprocess
import random
import string
import requests
import json
import time
import datetime
import pandas as pd

spark_hists_host='10.255.3.1' #'neu-3-1'
spark_hists_port=18080


def randomString(stringLength=8):
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(stringLength))


def configure_ceph_bw(playbook,nic,bw):
     process = subprocess.Popen(['ansible-playbook', playbook, "--extra-vars", '{"nic":%s,"rate":%s}'%(nic,bw)])
     process.wait()

def restart_rgw(playbook):
     process = subprocess.Popen(['ansible-playbook', playbook])
     process.wait()


def get_spark_apps():
    URL = "http://%s:%d/api/v1/applications"%(spark_hists_host,spark_hists_port)
    return requests.get(url = URL).json()

def get_spark_stages(app_id):
    URL = "http://%s:%d/api/v1/applications/%s/stages"%(spark_hists_host, spark_hists_port, app_id)
    count = 0
    while True:
        response = requests.get(url = URL)
        if response.status_code == 200:
            return response.json()
        time.sleep(5)
        count += 1
        if count == 10:
            raise NameError(response.text)



def pull_spark_app_stages_stats(app_id,app_name, app_runtime):
    app_stats = []
    stages = get_spark_stages(app_id)
    name = app_name.split('-')[0].split(':')[1]
    bw = app_name.split('-')[1].split(':')[1]
    dataset = app_name.split('-')[2].split(':')[1]
    stride = app_name.split('-')[3].split(':')[1]
    rep = app_name.split('-')[4].split(':')[1]


    for s in stages:
        submit_time = time.mktime(datetime.datetime.strptime(s['submissionTime'],
                                                     "%Y-%m-%dT%H:%M:%S.%fGMT").timetuple())
        start_time = time.mktime(datetime.datetime.strptime(s['firstTaskLaunchedTime'],
                                                     "%Y-%m-%dT%H:%M:%S.%fGMT").timetuple())
        completion_time = time.mktime(datetime.datetime.strptime(s['completionTime'],
                                                     "%Y-%m-%dT%H:%M:%S.%fGMT").timetuple())
        app_stats.append({'app_id': app_id, 'app_name': app_name, 'stage_id': s['stageId'], 
            'input_sz': s['inputBytes'], 'output_sz': s['outputBytes'], 
            'rdds': s['rddIds'], 'n_tasks': s['numTasks'], 'runtime': completion_time - submit_time,
            'name': name, 'bw': bw, 'dataset': dataset, 'stride': stride, 'rep': rep, 'app_time': app_runtime})
    return app_stats


def pull_spark_app_stats(app_name, statfile):
    applications = get_spark_apps(); 
    
    for app in applications:
        if app['name'] == app_name:
            app_runtime = app['attempts'][0]['duration']
            stats = pull_spark_app_stages_stats(app['id'], app_name, app_runtime)
            #with open(statfile, 'a+') as fd:
            df = pd.DataFrame(stats);
            df.to_csv(statfile, mode='a', header=False, index=False, columns=['app_id','app_name','stage_id','input_sz','output_sz','rdds','n_tasks','runtime','name','bw','dataset','stride','rep', 'app_time'])

def clear_spark_tmp_directory(playbook):
    process = subprocess.Popen(['ansible-playbook', playbook])
    process.wait()


def pull_pig_app_stats(app_name, statfile, statdata):
    string = statdata.decode('utf-8')
    app_meta = {'app_name': app_name}
    for i in app_name.split('-'):
        app_meta[i.split(':')[0]] = i.split(':')[1]
    app_meta['query_runtime'] = [int(ln.split('\t')[1]) for ln in string.split('Job DAG:')[1].split('\n') if 'times (sec):' in ln][0]
    app_meta['workflow_dag'] = [ln for ln in string.split('Job DAG:')[1].split('\n') if ln.startswith('job')]
    substring = [i for i in string.split('Success!')[1].split('Input(s):')[0].split('\n') if i][1:]
    df = pd.DataFrame([{**dict(zip(substring[0].split('\t'), s.split('\t'))), **app_meta} for s in substring[1:]])

    df = df.apply(pull_mapreduce_app_stats, axis=1)
    
    with open('runtime.pred.pig.stats.csv', 'a') as fd:
        df.to_csv(fd, header=False, index=False)
    pass


def pull_mapreduce_app_stats(data):
    tasks = get_executed_tasks(data['JobId']);
    
    stats = {
    'max_runtime' : 0,
    'min_runtime' : -1,
    'sum_runtime' : 0,
    'n_maps' : 0,
    'avg_runtime': 0}
    
    for t in tasks:
        if t['type'] == 'MAP':
            stats['n_maps'] += 1
            stats['sum_runtime'] += (t['elapsedTime']/1000)
            if t['elapsedTime'] > stats['max_runtime']:
                stats['max_runtime'] = t['elapsedTime']
            elif t['elapsedTime'] < stats['min_runtime'] or stats['min_runtime'] == -1:
                stats['min_runtime'] = t['elapsedTime']
    data['map_avg'] = round(stats['sum_runtime']/stats['n_maps'], 3)            
    data['map_min'] = stats['min_runtime']/1000
    data['map_max'] = stats['max_runtime']/1000
    
    job_info = get_executed_job(data['JobId']);
    data['runtime'] = (job_info['finishTime'] - job_info['startTime'])/1000
    data['queuetime'] = (job_info['startTime'] - job_info['submitTime'])/1000
    
    return data

def get_executed_job(job_id):
    host_name='neu-3-1'
    port=19888
    url = 'http://%s:%d/ws/v1/history/mapreduce/jobs/%s'%(host_name, port, job_id)
    r=requests.get(url)
    return r.json()['job']

def get_executed_tasks(job_id):
    host_name='neu-3-1'
    port=19888
    url = 'http://%s:%d/ws/v1/history/mapreduce/jobs/%s/tasks'%(host_name, port, job_id)
    r=requests.get(url)
    return r.json()['tasks']['task']


def update_statistics(framework, app_name, statfile, statdata=None, prefetch_results=None):
    df = pd.DataFrame(prefetch_results)
    with open('runtime.pred.spark.prefetch.csv', 'a') as fd:
        df.to_csv(fd, header=False, index=False)

    if framework == 'spark':
        with open('runtime.pred.spark.stats.csv', 'a') as fd:
            fd.write('%s\n'%(app_name))
        #return pull_spark_app_stats(app_name, statfile)
    elif framework == 'pig':
        return pull_pig_app_stats(app_name, statfile, statdata)
    elif framework == 'mapreduce':
        pull_mapreduce_app_stats(app_name, statfile)
