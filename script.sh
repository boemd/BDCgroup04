#!/bin/bash
for k in {10..200..10}
	do 
		echo $k : >> output.txt
		spark-submit —total-executor-cores 72 —executor-cores 8 —class it.unipd.dei.bdc1718.G04HM4 BigData-all.jar vecs-50-10000.txt $k >> output$k.txt
	done