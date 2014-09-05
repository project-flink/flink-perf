#!/bin/bash

echo "Running TPCH-3"

. ./configDefaults.sh

ARGS="$HDFS_TPCH3/lineitem.tbl $HDFS_TPCH3/customer.tbl $HDFS_TPCH3/orders.tbl $HDFS_TPCH3_OUT"
echo "running TPCH-3 with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP $TESTJOB_HOME"/flink-jobs/target/flink-jobs-*.jar" \
 -c com.github.projectflink.testPlan.TPCHQuery3 $ARGS

