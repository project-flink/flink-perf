#!/bin/bash

VERTEX_NUM=$1
EDGE_NUM=$2
if [[ -z "$EDGE_NUM" ]]; then
	VERTEX_NUM=100000
	EDGE_NUM=1000000
	echo "No argument passed. number of vertices set to 100 000, number of edges set to 1 000 000. Pass the number of vertices as the first argument and the number of edges as the second argument"
fi
echo "vertex_num=$VERTEX_NUM, edge_num=$EDGE_NUM"

echo "Generating Data that is required for running the tasks"

. ./configDefaults.sh

mkdir -p $FILES_DIRECTORY"/cp-data"

python CPDataGenerator.py $VERTEX_NUM $EDGE_NUM $FILES_CP_GEN_VERTEX $FILES_CP_GEN_EDGE


echo "done. find the generated file in $FILES_DIRECTORY/cp-data/"


