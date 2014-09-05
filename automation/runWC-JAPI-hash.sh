#!/bin/bash

echo "Running wordcount example"

. ./configDefaults.sh

ARGS="$HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -c org.apache.flink.example.java.wordcount.WordCountHashAgg -j $FILES_DIRECTORY/flink/flink-examples/flink-java-examples/target/flink-java-examples-*SNAPSHOT.jar -a $ARGS

