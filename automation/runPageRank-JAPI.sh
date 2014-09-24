#/bin/sh

echo "Running Page Rank"

. ./configDefaults.sh

ARGS="$HDFS_PAGERANK_PAGES $HDFS_PAGERANK_LINKS $HDFS_PAGERANK_OUT 1000000 10"
echo "running Page Rank with args $ARGS"

$FLINK_BUILD_HOME"/bin/flink" run -p $DOP $TESTJOB_HOME"/flink-jobs/target/flink-jobs-*-SNAPSHOT.jar" \
 -c com.github.projectflink.testPlan.PageRank $ARGS

