#/bin/sh

echo "Running KMeans"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS_POINTS $HDFS_KMEANS_CENTERS $HDFS_KMEANS_OUT 100"
echo "running KMeans with args $ARGS"

$FLINK_BUILD_HOME"/bin/flink" run -p $DOP $TESTJOB_HOME"/target/flink-perf-*.jar" \
 -c com.github.projectflink.testPlan.KMeansArbitraryDimension $ARGS

