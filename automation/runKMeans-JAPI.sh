#!/bin/bash

echo "Running wordcount example"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS_POINTS $HDFS_KMEANS_CENTERS $HDFS_KMEANS_OUT 15"
echo "running wc with args $ARGS"
$STRATOSPHERE_BUILD_HOME"/bin/stratosphere" run -p $DOP -j $STRATOSPHERE_BUILD_HOME/examples/stratosphere-java-examples-*-KMeans.jar $ARGS

