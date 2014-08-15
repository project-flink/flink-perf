#/bin/sh

echo "Running KMeans example"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS/point.txt $HDFS_KMEANS/center.txt $HDFS_KMEANS_OUT 1000"
echo "running cp with args $ARGS"

$FLINK_BUILD_HOME"/bin/flink" run -v -j $TESTJOB_HOME"/target/flink-perf-*.jar" \
 -c com.github.projectflink.testPlan.KMeansArbitraryDimension $ARGS

