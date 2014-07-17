#/bin/sh

echo "Running connected components example"

. ./configDefaults.sh

ARGS="$HDFS_CP/vertex.txt $HDFS_CP/edge.txt $HDFS_CP_OUT 1000 $DOP"
echo "running cp with args $ARGS"
$FLINK_BUILD_HOME"/bin/flink" run -v \
 $FLINK_BUILD_HOME/examples/flink-scala-examples-*-ConnectedComponents.jar $ARGS

