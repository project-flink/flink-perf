#!/bin/bash

echo "Running Spark wordcount example"

. ./configDefaults.sh

HOST=`hostname`
$SPARK_HOME/bin/spark-submit --master spark://$HOST:7077 \
 --deploy-mode cluster --class com.github.projectflink.spark.WordCount \
 /home/robert/flink-workdir/flink-perf/target/flink-perf-0.1-SNAPSHOT-SparkWC.jar \
 spark://$HOST:7077 $HDFS_WC $HDFS_SPARK_WC_OUT


