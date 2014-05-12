#/bin/sh

echo "Running wordcount example"

. ./config.sh

ARGS="$DOP $HDFS_WC $HDFS_WC_OUT"
echo "running wc with args $ARGS"
$STRATOSPHERE_BUILD_HOME"/bin/stratosphere" run -j $STRATOSPHERE_BUILD_HOME/examples/stratosphere-scala-examples-*-WordCount.jar -a $ARGS

