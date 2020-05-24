#!/usr/bin/python3
import config as cfg 
import math
import requests
import swift
import json 


def clear_cache(token=None):
    if not token:
        token = swift.get_token()

    url = 'http://%s:%d/swift/v1/'%(cfg.rgw_host, cfg.rgw_port)
    headers = {"KARIZ_FLUSH_CACHE":"1",
              "X-Auth-Token": token}

    r=requests.delete(url, headers=headers)
    print(r)


def fetch_object_partial(token, bucket_name, obj_name, ofs_s, ofs_e):
    print(bucket_name, obj_name)

    url = 'http://%s:%d/swift/v1/%s/%s'%(cfg.rgw_host, cfg.rgw_port, bucket_name, obj_name)
    headers = {"range":"bytes=%d-%d"%(ofs_s, ofs_e),
              "X-Auth-Token": token}
    r=requests.get(url, headers=headers)

    print(len(r.content), ofs_e - ofs_s)


def prefetch_dataset_stride(metadata, token, path, wave=-1, stride=0):
    # stride = -1 means prefetch the full object
    # wave = -1 means follow the partitions schema of original dataset
    cache_block_size = 4194304 # 4 MB

    path_element = path.split('/')

    meta_ptr = metadata
    for element in path_element:
        if element not in meta_ptr:
            return -1; # means could not prefetch this input
        meta_ptr = meta_ptr[element]['objs']

    for obj in meta_ptr:
        n_cache_blocks = meta_ptr[obj]['size']//cfg.cache_block_size


        ofs_s = (n_cache_blocks - stride)*cfg.cache_block_size if (stride) else 0
        ofs_e = meta_ptr[obj]['size']

        fetch_object_partial(token=token, bucket_name='data', obj_name=path+'/'+obj, ofs_s = ofs_s, ofs_e = ofs_e)


def get_dataset_metadata(metadata, token, path, wave=-1, stride=0):
    # stride = -1 means prefetch the full object
    # wave = -1 means follow the partitions schema of original dataset
    cache_block_size = 4194304 # 4 MB

    path_element = path.split('/')

    meta_ptr = metadata
    for element in path_element:
        if element not in meta_ptr:
            return -1; # means could not prefetch this input
        meta_ptr = meta_ptr[element]['objs']

    return meta_ptr




