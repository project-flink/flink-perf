#!/bin/bash

DIMENSION=$1
K=$2
NUM=$3
if [[ -z "$NUM" ]]; then
	DIMENSION=3
	K=8
	NUM=100000
	echo "No argument passed. Dimension set to 3, K set to 8, number of vertices set to 100 000. Pass the dimension as the first argument, k as the second argument and num of vertices as the last argument."
fi

echo "D=$DIMENSION, K=$K, NUM=$NUM"

echo "Generating Data that is required for running the tasks"

. ./configDefaults.sh

mkdir -p $FILES_DIRECTORY"/kmeans-data"

python KMeansDataGenerator.py $DIMENSION $K $NUM $FILES_KMEANS_GEN_POINT $FILES_KMEANS_GEN_CENTER

echo "done. find the generated file in $FILES_DIRECTORY/kmeans-data/"
