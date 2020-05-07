#!/usr/bin/python3
import sys
from paramiko import SSHClient


stride = sys.argv[1]



ssh = SSHClient()
ssh.load_system_host_keys()
ssh.connect('root@neu-3-41:/root')
ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command('/root/evict_from_cache.py ' + stride)
print(ssh_stdout) #print the output of ls command


