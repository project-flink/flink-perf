#!/bin/bash

echo "Running wordcount example"

. ./configDefaults.sh

ARGS="$DOP $HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -j $FLINK_BUILD_HOME/examples/flink-scala-examples-*-WordCount.jar -a $ARGS

