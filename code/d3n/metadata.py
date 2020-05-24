#!/usr/bin/python3
import requests
import swiftclient
import json 


def connect_swift(rgw_host, rgw_port, swift_user, swift_key):
    url = 'http://%s:%d/auth/1.0'%(rgw_host, rgw_port)

    conn = swiftclient.Connection(
            user=swift_user,
            key=swift_key,
            authurl=url)

    return conn;

def get_token(rgw_host, rgw_port, swift_user, swift_key):
    conn = connect_swift(rgw_host, rgw_port, swift_user, swift_key)
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

def load_metadata(rgw_host, rgw_port, swift_user, swift_key, bucket_name):
    conn = connect_swift(rgw_host, rgw_port, swift_user, swift_key);
    metadata = {}

    metadata_swift =  conn.get_container(bucket_name)[1]
    for data in metadata_swift:
        full_name = data['name'];

        if full_name.endswith('_SUCCESS') : continue;
        add_to_metadata_tree(metadata, data['name'], data['bytes'])
    return metadata
