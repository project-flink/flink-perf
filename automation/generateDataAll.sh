#!/bin/bash

cd data-generator

echo "Generating Word Count data."
./generateWCdata.sh 1000000

echo "Generating KMeans data."
./generateKMeansdata.sh

echo "Generating Connected Components data."
./generateCPdata.sh

echo "Generating Page Rank data."
./generatePRdata.sh 1000000 1000000

echo "Generating TPCH data."
./generateTestjobData.sh