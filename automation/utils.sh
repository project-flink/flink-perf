#!/bin/bash



createHDFSDirectory() {
	$HADOOP_BIN fs -test -d $HDFS_WORKING_DIRECTORY
	probe=$?
	if [ $probe -ne 0 ]; then
		echo "Directory $HDFS_WORKING_DIRECTORY does not exist. Trying to create it"
		$HADOOP_BIN fs -mkdir -p $HDFS_WORKING_DIRECTORY
	fi
}
