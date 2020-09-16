#!/usr/bin/python

# Import socket module 
import socket 
import sys
import json
import psutil
import time
import threading
import signal
from time import gmtime, strftime


class GracefulKiller:
  kill_now = False
  def __init__(self):
    signal.signal(signal.SIGINT, self.exit_gracefully)
    signal.signal(signal.SIGTERM, self.exit_gracefully)

  def exit_gracefully(self,signum, frame):
    self.kill_now = True


def collect_stats():
    cpu_stats = psutil.cpu_percent(interval=1, percpu=True)
    network_stats = psutil.net_io_counters(pernic=True)
    disk_stats = psutil.disk_io_counters(perdisk=False)
    mem_stats = psutil.virtual_memory() 
    
    stats = dict();
    stats['time'] = strftime("%Y-%m-%d %H:%M:%S", gmtime())
    stats['cpu'] = cpu_stats
    stats['dsk'] = disk_stats
    stats['mem'] = mem_stats
    stats['net'] = network_stats
    return stats; 

def Main(): 
   killer = GracefulKiller()
   
   host = '128.31.24.168'
   port = 4964

   s = socket.socket(socket.AF_INET,socket.SOCK_STREAM) 
   s.connect((host,port)) 

   while True:
      # message sent to server
      stats = collect_stats();
      message = str(stats)

      s.send(("lenght:"+str(len(message))).encode('ascii'))

      data = s.recv(1024)
      print(data.decode('ascii'))
      if data.decode('ascii') == "ready":
         print("Sever Ready to send stats");
         s.send(message.encode('ascii')) 

      # messaga received from server 
      data = s.recv(1024) 

      print('Received from the server :',str(data.decode('ascii'))) 

      if killer.kill_now:
         s.send(("done").encode('ascii'))
         s.close() 
         break

      time.sleep(5);
   

   # close the connection 

if __name__ == '__main__': 
   Main()
   
