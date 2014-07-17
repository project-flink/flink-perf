#!/bin/bash

echo "Running TPCH-10"

. ./configDefaults.sh

ARGS="$HDFS_TPCH10/customer.tbl $HDFS_TPCH10/orders.tbl $HDFS_TPCH10/lineitem.tbl $HDFS_TPCH10/nation.tbl $HDFS_TPCH10_OUT"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -j $FLINK_BUILD_HOME/examples/flink-java-examples-*-TPCHQuery10.jar $ARGS

