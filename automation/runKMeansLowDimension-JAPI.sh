#/bin/sh

echo "Running KMeans Low Dimension"

. ./configDefaults.sh

ARGS="$HDFS_KMEANS/point-low.txt $HDFS_KMEANS/center-low.txt $HDFS_KMEANS_OUT/low 100"
echo "running KMeans with args $ARGS"

$FLINK_BUILD_HOME"/bin/flink" run -p $DOP $TESTJOB_HOME"/target/flink-perf-*.jar" \
 -c com.github.projectflink.testPlan.KMeansArbitraryDimension $ARGS

