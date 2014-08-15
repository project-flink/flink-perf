#!/bin/bash

echo "Running TPCH-3"

. ./configDefaults.sh

ARGS="$HDFS_TPCH3/lineitem.tbl $HDFS_TPCH3/customer.tbl $HDFS_TPCH3/orders.tbl $HDFS_TPCH3_OUT"
echo "running TPCH-3 with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP $FLINK_BUILD_HOME/examples/flink-java-examples-*-TPCHQuery3.jar $ARGS

