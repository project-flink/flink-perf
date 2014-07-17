#/bin/sh

echo "Running connected components example"

. ./configDefaults.sh

ARGS="$HDFS_CP/vertex.txt $HDFS_CP/edge.txt $HDFS_CP_OUT 1000"
echo "running cp with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -v \
 $FLINK_BUILD_HOME/examples/flink-java-examples-*-ConnectedComponents.jar $ARGS

