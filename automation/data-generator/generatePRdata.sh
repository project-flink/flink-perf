#!/bin/bash
cd ..
. ./configDefaults.sh

VERTEX_NUM=$1
EDGE_NUM=$2
if [[ -z "$EDGE_NUM" ]]; then
	VERTEX_NUM=100000
	EDGE_NUM=1000000
	echo "No argument passed. number of vertices set to 100 000, number of edges set to 1 000 000. Pass the number of vertices as the first argument and the number of edges as the second argument"
fi
echo "vertex_num=$VERTEX_NUM, edge_num=$EDGE_NUM"

echo "Generating Data that is required for running the tasks"

mkdir -p $FILES_DIRECTORY"/pagerank-data"

python data-generator/RandomGraphGenerator.py $VERTEX_NUM $EDGE_NUM 0 $FILES_PAGERANK_GEN_VERTEX $FILES_PAGERANK_GEN_EDGE


echo "done. find the generated file in $FILES_DIRECTORY/pagerank-data/"


