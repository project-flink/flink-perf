#!/bin/bash

echo "Running Spark KMeans Low Dimension"

. ./configDefaults.sh

HOST=`hostname`
ARGS="spark://$HOST:7077 $HDFS_KMEANS/point-low.txt $HDFS_KMEANS/center-low.txt $HDFS_SPARK_KMEANS_OUT/low 100"
echo "running KMeans with args $ARGS"

$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.KMeansArbitraryDimension \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*-SNAPSHOT.jar` \
 $ARGS