#!/bin/bash

echo "Running Spark wordcount example"

. ./configDefaults.sh

HOST=`hostname`
$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --deploy-mode client --class com.github.projectflink.spark.WordCountGrouping \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*-SparkWC.jar` \
 spark://$HOST:7077 $HDFS_WC $HDFS_SPARK_WC_OUT 400


