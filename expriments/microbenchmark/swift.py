#!/usr/bin/python3
import config as cfg 
import requests
import swiftclient
import json 


def connect_swift():
    url = 'http://%s:%d/auth/1.0'%(cfg.rgw_host, cfg.rgw_port)

    conn = swiftclient.Connection(
            user=cfg.swift_user,
            key=cfg.swift_key,
            authurl=url)

    return conn;

def get_token():
    conn = connect_swift()
    token = conn.get_auth()[1]
    print('RGW token:', token)
    return token;


def add_to_metadata_tree(metadata, name, size):
    path_element = name.split('/')

    meta_ptr = metadata
    for element in path_element:
        if element not in meta_ptr:
            meta_ptr[element] = {'objs': {}, 'size': 0 }
        meta_ptr[element]['size'] += size
        meta_ptr = meta_ptr[element]['objs']

def load_metadata():
    conn = connect_swift();
    metadata = {}

    metadata_swift =  conn.get_container(cfg.bucket_name)[1]
    for data in metadata_swift:
        full_name = data['name'];

        if full_name.endswith('_SUCCESS') : continue;
        add_to_metadata_tree(metadata, data['name'], data['bytes'])
    return metadata
