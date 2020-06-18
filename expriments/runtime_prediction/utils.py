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
        
        print(response.text)
        time.sleep(5)
        count += 1
        if count == 10:
            raise NameError("History server is not accessible")



def pull_spark_app_stages_stats(app_id,app_name):
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
            'name': name, 'bw': bw, 'dataset': dataset, 'stride': stride, 'rep': rep})
    return app_stats


def pull_spark_app_stats(app_name, statfile):
    applications = get_spark_apps(); 
    
    for app in applications:
        if app['name'] == app_name:
            stats = pull_spark_app_stages_stats(app['id'], app_name)
            with open(statfile, 'a+') as fd:
                df = pd.DataFrame(stats);
                df.to_csv(fd, header=False, index=False)



def pull_pig_app_stats(app_name, statfile):
    pass



def pull_mapreduce_app_stats(app_name, statfile):
    pass



def update_statistics(framework, app_name, statfile):
    if framework == 'spark':
        return pull_spark_app_stats(app_name, statfile)
    elif framework == 'pig':
        pull_pig_app_stats(app_name, statfile)
    elif framework == 'mapreduce':
        pull_mapreduce_app_stats(app_name, statfile)
