#!/bin/bash

echo "Running wordcount example"

. ./configDefaults.sh

ARGS="$HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$STRATOSPHERE_BUILD_HOME"/bin/stratosphere" run -p $DOP -j $STRATOSPHERE_BUILD_HOME/examples/stratosphere-java-examples-*-WordCount.jar $ARGS

