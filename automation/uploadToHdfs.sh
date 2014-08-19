#!/bin/bash

echo "Uploading available test data to hdfs"

. ./configDefaults.sh
. ./utils.sh


createHDFSDirectory

echo "checking for Word Count data"
if [[ -e "$FILES_WC_GEN" ]]; then
	echo "found generated wordcount data"
	$HADOOP_BIN fs -test -e $HDFS_WC
	probe=$?
	if [ $probe -ne 1 ]; then
		echo "There is already wordcount data in hdfs. Skipping ...";
	else
		echo "Uploading to hdfs"
		echo "doing $HADOOP_BIN fs -copyFromLocal $FILES_WC_GEN $HDFS_WC"
		$HADOOP_BIN fs -copyFromLocal $FILES_WC_GEN $HDFS_WC
	fi
fi 

echo "checking for Connected Components data"
if [[ -e "$FILES_CP_GEN_VERTEX" ]]; then
	echo "found generated connected components data"
	$HADOOP_BIN fs -test -e $HDFS_CP
	probe=$?
	if [ $probe -ne 1 ]; then
		echo "There is already connected components data in hdfs. Skipping ...";
	else
		echo "Uploading to hdfs"
		$HADOOP_BIN fs -mkdir -p $HDFS_CP"/"
		$HADOOP_BIN fs -copyFromLocal $FILES_CP_GEN_VERTEX $HDFS_CP"/"
		$HADOOP_BIN fs -copyFromLocal $FILES_CP_GEN_EDGE $HDFS_CP"/"
	fi
fi 

echo "checking for KMeans data"
if [[ -e "$FILES_KMEANS_LOW_GEN_POINT" ]]; then
	echo "found generated KMeans data"
	$HADOOP_BIN fs -test -e $HDFS_KMEANS
	probe=$?
	if [ $probe -ne 1 ]; then
		echo "There is already KMeans data in hdfs. Skipping ...";
	else
		echo "Uploading to hdfs"
		$HADOOP_BIN fs -mkdir -p $HDFS_KMEANS"/"
		$HADOOP_BIN fs -copyFromLocal $FILES_KMEANS_LOW_GEN_POINT $HDFS_KMEANS"/"
		$HADOOP_BIN fs -copyFromLocal $FILES_KMEANS_LOW_GEN_CENTER $HDFS_KMEANS"/"
		$HADOOP_BIN fs -copyFromLocal $FILES_KMEANS_HIGH_GEN_POINT $HDFS_KMEANS"/"
		$HADOOP_BIN fs -copyFromLocal $FILES_KMEANS_HIGH_GEN_CENTER $HDFS_KMEANS"/"		
	fi
fi 


echo "checking for tpch data"
if [[ -e "$TESTJOB_DATA/customer.tbl" ]]; then
	echo "found data for the testjob"
	$HADOOP_BIN fs -test -e $HDFS_TESTJOB/customer.tbl
	probe=$?
	echo "probe=$probe"
	if [ $probe -ne 1 ]; then
		echo "There is already testjob data in hdfs. Skipping ...";
	else
		echo "Uploading to hdfs"
		$HADOOP_BIN fs -mkdir -p $HDFS_TESTJOB"/"
		$HADOOP_BIN fs -copyFromLocal $TESTJOB_DATA/*.tbl $HDFS_TESTJOB"/"
		$HADOOP_BIN fs -copyFromLocal $TESTJOB_DATA/*.avro $HDFS_TESTJOB"/"
	fi
fi



