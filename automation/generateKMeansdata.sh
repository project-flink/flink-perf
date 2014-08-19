#!/bin/bash

. ./configDefaults.sh

NUM1=$1
NUM2=$2
if [[ -z "$NUM" ]]; then
	NUM1=100000
	NUM2=1000
	echo "No argument passed. Number of low dimension vertices set to 100 000, number of high dimension vertices set to 1000."
fi

echo "Generating Data that is required for running the tasks: Dimension=3, K=8, NUM=$NUM1"


mkdir -p $FILES_DIRECTORY"/kmeans-data"

python KMeansDataGenerator.py 3 8 $NUM1 $FILES_KMEANS_LOW_GEN_POINT $FILES_KMEANS_LOW_GEN_CENTER

echo "Generating Data that is required for running the tasks: Dimension=1000, K=800, NUM=$NUM2"

python KMeansDataGenerator.py 1000 800 $NUM2 $FILES_KMEANS_HIGH_GEN_POINT $FILES_KMEANS_HIGH_GEN_CENTER

echo "done. find the generated file in $FILES_DIRECTORY/kmeans-data/"
