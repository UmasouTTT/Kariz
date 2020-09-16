import string
import random

input_min = 1
input_max = 256

index_min = 11
index_max = 20

fpath = '/local0/Kariz/expriments/simulator/multidag/config/inputs.csv'

input_str=''
for c in string.ascii_lowercase:
    for i in range(index_min, index_max):
        input_str += '%s%d,%d\n'%(c, i, random.randint(input_min, input_max))
        
with open(fpath, 'a') as fd:
    fd.write(input_str)
