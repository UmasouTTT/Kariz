#!/bin/sh

for i in 1 2 3
do
	bin/workloads/micro/terasort/hadoop/run.sh

	echo $i
done
