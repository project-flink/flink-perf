#!/bin/bash

echo "Running Spark KMeans Low Dimension"

. ./configDefaults.sh

HOST=`hostname`
ARGS="spark://$HOST:7077 $HDFS_KMEANS_PERF_POINTS $HDFS_KMEANS_PERF_CENTERS $HDFS_SPARK_KMEANS_OUT $ITERATIONS"
echo "running KMeans with args $ARGS"

$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.KMeansArbitraryDimension \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*-All.jar` $ARGS