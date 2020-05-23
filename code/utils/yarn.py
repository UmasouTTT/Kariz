#!/usr/bin/python3

import requests
import json



def get_running_applications():
    host_name='neu-5-1'
    port=8088
    url = 'http://%s:%d/ws/v1/cluster/apps?states=running'%(host_name, port)
    headers = {'Accept':'application/json'}

    r=requests.get(url, headers=headers)

    return r.json()['apps']['app'] if r.json()['apps'] else []


def get_appname():
    running_apps = get_running_applications()


    #print(running_apps)
    # only return the first job 
    job_ids = ''
    for app in running_apps:
        job_ids = app['id'].replace('application', 'job')

    return job_ids

