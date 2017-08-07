#!/bin/bash

batchsize=5
increment=1	
cnt=1

while (($cnt<=1))
do
	./smr-ssd-cache 908 1 0 1 0 50000 50000 $batchsize > /home/outputs/lru_batch/b908_src_wo_$(($batchsize))_NOSORT
	batchsize=$(($batchsize+$increment))
	cnt=$(($cnt+1))
	rm -f /dev/shm/*
done
