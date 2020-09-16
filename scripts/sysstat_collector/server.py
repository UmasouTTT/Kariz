#!/usr/bin/python


# import socket programming library 
import socket 
import json

# import thread module 
from _thread import *
import threading 

print_lock = threading.Lock() 

# thread fuction 
def threaded(c, addr): 
   # lock acquired by client 
   print_lock.acquire()
   print('Connected to :', addr[0], ':', addr[1])
   print_lock.release()
   stat_file = "/mnt/temp/state_" + addr[0] + "_" + str(addr[1]); 
   lenght = 0;
   
   wait_for_stat_flag = 1
   wait_for_stat_data = 2
   status = wait_for_stat_flag;
   done = False
   with open(stat_file, "w") as fd:
      while not done: 

         if status == wait_for_stat_flag:
            print("Wait for Start Flag")
            # data received from client 
            data = c.recv(1024).decode('ascii')
            if data.startswith('lenght:'):
               lenght = int(data.split(":")[1])
               c.send(("ready").encode('ascii'));
               status = wait_for_stat_data 
            else: #data.startswith('done'):
               print_lock.acquire()
               print('Bye')
               # lock released on exit 
               print_lock.release() 
               c.close()
               done = True
         elif status == wait_for_stat_data:
            # data received from client 
            print("Wait for Data")
            data = c.recv(lenght);
            #stats = json.loads(data.decode('ascii'))
            stats = data.decode('ascii')
            print(stats)
            fd.write(stats)
           
            # send back reversed string to client 
            c.send(str("done").encode('ascii'))
            status = wait_for_stat_flag; 
      
      # connection closed 


def Main(): 
	host = "" 

	# reverse a port on your computer 
	# in our case it is 12345 but it 
	# can be anything 
	port = 4964
	s = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
	s.bind((host, port)) 
	print("socket binded to post", port) 

	# put the socket into listening mode 
	s.listen(5) 
	print("socket is listening") 

	# a forever loop until client wants to exit 
	while True: 

		# establish connection with client 
		c, addr = s.accept() 

		# Start a new thread and return its identifier 
		start_new_thread(threaded, (c, addr, )) 
	s.close() 


if __name__ == '__main__': 
	Main() 
