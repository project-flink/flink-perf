#!/bin/bash

echo "Running wordcount japi without combine example"

. ./configDefaults.sh

ARGS="$HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -c com.github.projectflink.testPlan.WordCountWithoutCombine -j $TESTJOB_HOME"/target/flink-perf-*.jar" $ARGS
