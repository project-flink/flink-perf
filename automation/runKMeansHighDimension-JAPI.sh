#/bin/sh

echo "Running KMeans High Dimension"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS/point-high.txt $HDFS_KMEANS/center-high.txt $HDFS_KMEANS_OUT/high 100"
echo "running KMeans with args $ARGS"

$FLINK_BUILD_HOME"/bin/flink" run -p $DOP $TESTJOB_HOME"/target/flink-perf-*.jar" \
 -c com.github.projectflink.testPlan.KMeansArbitraryDimension $ARGS

