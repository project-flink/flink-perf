#!/bin/bash

echo "Running readonly for flink"

. ./configDefaults.sh

echo "running wc with dop=$DOP"
$FLINK_BUILD_HOME"/bin/flink" run -c com.github.projectflink.testPlan.Readonly -p $DOP $TESTJOB_HOME"/flink-jobs/target/flink-jobs-*-SNAPSHOT.jar" $HDFS_WC