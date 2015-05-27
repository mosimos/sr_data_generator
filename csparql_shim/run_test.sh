#!/bin/sh

if [ $# == 5 ]; then
	../feeder/simple_feeder.py $1 | java -jar target/CsparqlShim-0.0.1.jar $2 $3 $4 $5
else
	echo "not enough parameters"
	echo "usage: run_test <streaming_data_file> <static_data_file> <query_file> <streaming_pause_period> <triple_pause_period>"
fi

