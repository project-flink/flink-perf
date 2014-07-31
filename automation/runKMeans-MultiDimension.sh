#!/bin/bash

echo "Running Multi-Dimension KMeans example"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS_POINTS $HDFS_KMEANS_CENTERS $HDFS_KMEANS_OUT 15"
echo "running Multi-Dimension KMeans with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP 
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -c org.apache.flink.test.testPlan.KMeansArbitraryDimension -j $TESTJOB_HOME"/target/testjob-*.jar" $ARGS

