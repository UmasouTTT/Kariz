Cache for Maximum Runtime reduction (CMR)


This algorithm manages the io and cache when we have limited bandwidth and cache space


The idea is to
\t 1. estimate the runtime of each job in the DAG.
\t 2. understand the stages.  
\t 3. 
\t 3. build a score board for the DAG. for each stage on score board and then link them together. 
The idea is to assume a score-board for bandwidth.   

For now first build the score board. 
