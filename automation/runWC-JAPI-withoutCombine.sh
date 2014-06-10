#!/bin/bash

echo "Running wordcount japi without combine example"

. ./configDefaults.sh

ARGS="$HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$STRATOSPHERE_BUILD_HOME"/bin/stratosphere" run -p $DOP -c eu.stratosphere.test.testPlan.WordCountWithoutCombine -j $TESTJOB_HOME"/target/testjob-*.jar" $ARGS

