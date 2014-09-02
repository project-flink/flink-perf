#/bin/sh

echo "Running Page Rank"

. ./configDefaults.sh

ARGS="$HDFS_PAGERANK_PAGES $HDFS_PAGERANK_LINKS $HDFS_PAGERANK_OUT 100000 10"
echo "running Page Rank with args $ARGS"

$FLINK_BUILD_HOME"/bin/flink" run -p $DOP -j $FLINK_BUILD_HOME/examples/flink-java-examples-*-PageRankBasic.jar $ARGS

