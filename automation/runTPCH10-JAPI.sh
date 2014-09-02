#!/bin/bash

echo "Running TPCH-10"

. ./configDefaults.sh

ARGS="$HDFS_TPCH10/customer.tbl $HDFS_TPCH10/orders.tbl $HDFS_TPCH10/lineitem.tbl $HDFS_TPCH10/nation.tbl $HDFS_TPCH10_OUT"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -j  workdir/flink/flink-examples/flink-java-examples/target/flink-java-examples-0.6-incubating-SNAPSHOT.jar -c org.apache.flink.example.java.relational.TPCHQuery10 $ARGS

