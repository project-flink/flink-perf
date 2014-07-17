#!/bin/bash

echo "Running wordcount japi without combine example"

. ./configDefaults.sh

ARGS="$HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -c org.apache.flink.test.testPlan.WordCountWithoutCombine -j $TESTJOB_HOME"/target/testjob-*.jar" $ARGS

