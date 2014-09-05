#!/bin/bash

echo "Running Spark KMeans High Dimension"

. ./configDefaults.sh

HOST=`hostname`
ARGS="spark://$HOST:7077 $HDFS_KMEANS/point-high.txt $HDFS_KMEANS/center-high.txt $HDFS_SPARK_KMEANS_OUT/high 100"
echo "running KMeans with args $ARGS"

$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.KMeansArbitraryDimension \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*.jar` \
 $ARGS

