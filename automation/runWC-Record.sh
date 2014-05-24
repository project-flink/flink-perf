#!/bin/bash

echo "Running wordcount example"

. ./configDefaults.sh

ARGS="$DOP $HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$STRATOSPHERE_BUILD_HOME"/bin/stratosphere" run -j $STRATOSPHERE_BUILD_HOME/examples/stratosphere-tests-*-tests.jar -c eu.stratosphere.test.recordJobs.wordcount.WordCount -a $ARGS

