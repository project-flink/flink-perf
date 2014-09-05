#!/bin/bash

echo "Running Spark wordcount example"

. ./configDefaults.sh

HOST=`hostname`
$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.Readonly \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*-All.jar` \
 spark://$HOST:7077 $HDFS_WC


