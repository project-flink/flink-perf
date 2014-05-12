#/bin/sh

. ./config.sh
. ./utils.sh

echo "Showing contents of HDFS_WORKING_DIRECTORY=$HDFS_WORKING_DIRECTORY"


createHDFSDirectory

$HADOOP_BIN fs -lsr $HDFS_WORKING_DIRECTORY
