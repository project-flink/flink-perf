#/bin/sh

echo "Running KMeans High Dimension"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS_PERF_POINTS $HDFS_KMEANS_PERF_CENTERS $HDFS_KMEANS_OUT $ITERATIONS"
echo "running KMeans with args $ARGS"

$FLINK_BUILD_HOME"/bin/flink" run -p $DOP $TESTJOB_HOME"/flink-jobs/target/flink-jobs-*.jar" \
 -c com.github.projectflink.testPlan.KMeansArbitraryDimension $ARGS

