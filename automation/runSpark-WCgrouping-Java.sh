#!/bin/bash

echo "Running Spark wordcount grouping example"

. ./configDefaults.sh

HOST=`hostname`
$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.WordCountGrouping \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*-SNAPSHOT.jar` \
 spark://$HOST:7077 $HDFS_WC $HDFS_SPARK_WC_OUT 1


