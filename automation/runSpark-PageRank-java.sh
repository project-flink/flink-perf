#!/bin/bash

echo "Running Spark Page Rank"

. ./configDefaults.sh

HOST=`hostname`
ARGS="spark://$HOST:7077 $HDFS_PAGERANK_PAGES $HDFS_PAGERANK_LINKS $HDFS_SPARK_PAGERANK_OUT 1000000 10"
echo "running page rank with args $ARGS"

$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.PageRank \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*-SNAPSHOT.jar` \
 $ARGS