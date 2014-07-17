#!/bin/bash

echo "Running wordcount example"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS_POINTS $HDFS_KMEANS_CENTERS $HDFS_KMEANS_OUT 15"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -j $FLINK_BUILD_HOME/examples/flink-java-examples-*-KMeans.jar $ARGS

