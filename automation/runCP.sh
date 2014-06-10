#/bin/sh

echo "Running connected components example"

. ./configDefaults.sh

ARGS="$HDFS_CP/vertex.txt $HDFS_CP/edge.txt $HDFS_CP_OUT 1000 $DOP"
echo "running cp with args $ARGS"
$STRATOSPHERE_BUILD_HOME"/bin/stratosphere" run -v \
 $STRATOSPHERE_BUILD_HOME/examples/stratosphere-scala-examples-*-ConnectedComponents.jar $ARGS

