#!/bin/bash

echo "Uploading available test data to hdfs"

. ./configDefaults.sh
. ./utils.sh


createHDFSDirectory

echo "checking for wc data"
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



