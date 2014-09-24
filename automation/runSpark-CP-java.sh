#!/bin/bash

echo "Running Spark Connected Components"

. ./configDefaults.sh

HOST=`hostname`
ARGS="spark://$HOST:7077 $HDFS_CP/vertex.txt $HDFS_CP/edge.txt $HDFS_SPARK_CP_OUT 1000"
echo "running connected components with args $ARGS"

$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.ConnectedComponents \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*-SNAPSHOT.jar` \
 $ARGS