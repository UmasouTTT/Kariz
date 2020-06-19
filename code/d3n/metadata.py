#!/usr/bin/python3
import requests
import swiftclient
import json 
import d3n.config as cfg

class ObjectStore():
   def __init__(self):
        self.conn = self.connect_swift(); 
        self.token = self.get_token();
        self.metadata = {}


   def connect_swift(self):
       url = 'http://%s:%d/auth/1.0'%(cfg.rgw_host, cfg.rgw_port)   
       conn = swiftclient.Connection(
               user=cfg.swift_user,
               key=cfg.swift_key,
               authurl=url)
       return conn;


   def get_token(self):
       token = self.conn.get_auth()[1]
       return token;
   
   
   def add_to_metadata_tree(self, metadata, name, size):
       path_element = name.split('/')
       meta_ptr = metadata
       for element in path_element:
           if element not in meta_ptr:
               meta_ptr[element] = {'objs': {}, 'size': 0 }
           meta_ptr[element]['size'] += size
           meta_ptr = meta_ptr[element]['objs']
   

   def load_metadata(self):
       metadata = {}
       metadata_swift =  self.conn.get_container(cfg.bucket_name)[1]
       for data in metadata_swift:
           full_name = data['name'];
   
           if full_name.endswith('_SUCCESS') : continue;
           self.add_to_metadata_tree(metadata, data['name'], data['bytes'])
       self.metadata = metadata
       return metadata


   def clear_cache(self):
       token = self.get_token()
       print('Clear the cache')
       url = 'http://%s:%d/swift/v1/'%(cfg.rgw_host, cfg.rgw_port)
       headers = {"KARIZ_FLUSH_CACHE":"1",
                 "X-Auth-Token": token}
   
       r=requests.delete(url, headers=headers)
   
   
   def fetch_object_partial(self, bucket_name, obj_name, ofs_s, ofs_e):
       print('bucket_name', bucket_name, 'obj_name', obj_name, ', start offset', ofs_s, 'end offset', ofs_e)
       url = 'http://%s:%d/swift/v1/%s/%s'%(cfg.rgw_host, cfg.rgw_port, bucket_name, obj_name)
       headers = {"range":"bytes=%d-%d"%(ofs_s, ofs_e),
                 "X-Auth-Token": self.token}
       r=requests.get(url, headers=headers)

   
   def prefetch_dataset_stride(self, path, wave=-1, stride=0):
       cache_block_size = 4194304 # 4 MB
       path_element = path.split('/')
       meta_ptr = self.metadata
       for element in path_element:
           if element not in meta_ptr:
               return -1; # means could not prefetch this input
           meta_ptr = meta_ptr[element]['objs']
       for obj in meta_ptr:
           n_cache_blocks = meta_ptr[obj]['size']//cfg.cache_block_size
           ofs_s = (n_cache_blocks - stride)*cfg.cache_block_size if (stride!= -1) else 0
           ofs_e = meta_ptr[obj]['size']
           self.fetch_object_partial(bucket_name=cfg.bucket_name, obj_name=path+'/'+obj, ofs_s = ofs_s, ofs_e = ofs_e)


   def prefetch_dataset_map_stride(self, path, yarn_map_byte, stride=0):
       cache_block_size = 4194304 # 4 MB
       path_element = path.split('/')
       meta_ptr = self.metadata
       for element in path_element:
           if element not in meta_ptr:
               return -1; # means could not prefetch this input
           meta_ptr = meta_ptr[element]['objs']
       print('Metaptr', meta_ptr)
       for obj in meta_ptr:
           n_maps = math.ceil(meta_ptr[obj]['size']/yarn_map_byte) if meta_ptr[obj]['size'] > yarn_map_byte else 1


           map_byte = yarn_map_byte if meta_ptr[obj]['size'] > yarn_map_byte else meta_ptr[obj]['size']


           print('n_maps for this block is', n_maps, ', bytes per mapper', map_byte)

           for i in range(0, n_maps):
               n_cache_blocks = map_byte//cfg.cache_block_size
               if n_cache_blocks < stride:
                   n_cache_blocks = stride

               ofs_s = i*map_byte + (n_cache_blocks - stride)*cfg.cache_block_size if (stride!= -1) else i*map_byte
               ofs_e = (i+1)*map_byte if ((i+1)*map_byte <  meta_ptr[obj]['size']) else meta_ptr[obj]['size']
               self.fetch_object_partial(bucket_name=cfg.bucket_name, obj_name=path+'/'+obj, ofs_s = ofs_s, ofs_e = ofs_e)

   
   
   def get_dataset_metadata(self,path, wave=-1, stride=0):
       path_element = path.split('/')
       meta_ptr = self.metadata
       for element in path_element:
           if element not in meta_ptr:
               return -1; # means could not prefetch this input
           meta_ptr = meta_ptr[element]['objs']
       return meta_ptr
