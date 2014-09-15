#!/bin/bash

echo "Running Spark TPCH Query 3"

. ./configDefaults.sh

HOST=`hostname`
ARGS="spark://$HOST:7077 $HDFS_TPCH3/lineitem.tbl $HDFS_TPCH3/customer.tbl $HDFS_TPCH3/orders.tbl $HDFS_SPARK_TPCH3_OUT"
echo "running TPCH Query 3 with args $ARGS"

$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --class com.github.projectflink.spark.TPCHQuery3 \
 `ls "$TESTJOB_HOME"/spark-jobs/target/spark-jobs-*.jar` \
 $ARGS