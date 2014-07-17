#!/bin/bash

echo "Running wordcount example"

. ./configDefaults.sh

ARGS="$HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -j $FLINK_BUILD_HOME/examples/flink-java-examples-*-WordCount.jar $ARGS

