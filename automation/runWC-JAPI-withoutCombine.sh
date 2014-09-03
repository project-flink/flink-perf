#!/bin/bash

echo "Running wordcount japi without combine example"

. ./configDefaults.sh

echo "running wc with args"
$FLINK_BUILD_HOME"/bin/flink" run -c com.github.projectflink.testPlan.WordCountWithoutCombine -p $DOP $TESTJOB_HOME"/flink-jobs/target/flink-jobs-*-SNAPSHOT.jar" $HDFS_WC $HDFS_WC_OUT