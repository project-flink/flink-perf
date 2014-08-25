#!/bin/bash

# This is a simple bash file containing configuration values as variables

# the repo must be called flink
GIT_REPO=https://github.com/apache/incubator-flink.git
GIT_BRANCH=master

# the repo must be called testjob
TESTJOB_REPO=https://github.com/project-flink/flink-perf.git
TESTJOB_BRANCH=master

YARN=false
YARN_SESSION_CONF="-n 2 -jm 500 -tm 500"

OS=`uname -s`

# has to be a absolute path!
FILES_DIRECTORY=`pwd`/workdir
HDFS_WORKING_DIRECTORY=file:///tmp/flink-tests
if [ "$OS" == 'Linux' ]; then
    RAND=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1`
elif [ "$OS" == 'Darwin' ]; then
    RAND=`LC_CTYPE=C tr -dc 'a-zA-Z0-9' < /dev/urandom | fold -w 12 | head -n 1`
else
    echo "System $OS is not supported"
fi
MVN_BIN=mvn

#custom mvn flags (most likely -Dhadoop.profile=2 )
CUSTOM_FLINK_MVN=""
if [[ $YARN == "true" ]]; then
	CUSTOM_FLINK_MVN=" -Dhadoop.profile=2 "
fi

CUSTOM_TESTJOB_MVN=""

HADOOP_BIN="hadoop"

# FLINK Directory
FLINK_BUILD_HOME=$FILES_DIRECTORY"/flink-build"

# General Stuff
DOP=1

# Wordcount
FILES_WC_GEN=$FILES_DIRECTORY"/wc-data/generated-wc.txt"
HDFS_WC=$HDFS_WORKING_DIRECTORY"/wc-in"
HDFS_WC_OUT=$HDFS_WORKING_DIRECTORY"/wc-out-"$RAND

#Connected Component
FILES_CP_GEN_VERTEX=$FILES_DIRECTORY"/cp-data/vertex.txt"
FILES_CP_GEN_EDGE=$FILES_DIRECTORY"/cp-data/edge.txt"
HDFS_CP=$HDFS_WORKING_DIRECTORY"/cp-in"
HDFS_CP_VERTEX=$HDFS_CP"/vertex.txt"
HDFS_CP_EDGE=$HDFS_CP"/edge.txt"
HDFS_CP_OUT=$HDFS_WORKING_DIRECTORY"/cp-out-"$RAND

#KMeans
FILES_KMEANS_LOW_GEN_POINT=$FILES_DIRECTORY"/kmeans-data/point-low.txt"
FILES_KMEANS_LOW_GEN_CENTER=$FILES_DIRECTORY"/kmeans-data/center-low.txt"
FILES_KMEANS_HIGH_GEN_POINT=$FILES_DIRECTORY"/kmeans-data/point-high.txt"
FILES_KMEANS_HIGH_GEN_CENTER=$FILES_DIRECTORY"/kmeans-data/center-high.txt"
HDFS_KMEANS=$HDFS_WORKING_DIRECTORY"/kmeans-in"
HDFS_KMEANS_OUT=$HDFS_WORKING_DIRECTORY"/kmeans-out-"$RAND
HDFS_KMEANS_POINTS=$HDFS_WORKING_DIRECTORY"/kmeans-points"
HDFS_KMEANS_CENTERS=$HDFS_WORKING_DIRECTORY"/kmeans-centers"

#Page Rank
FILES_PAGERANK_GEN_VERTEX=$FILES_DIRECTORY"/pagerank-data/vertex.txt"
FILES_PAGERANK_GEN_EDGE=$FILES_DIRECTORY"/pagerank-data/edge.txt"
HDFS_PAGERANK=$HDFS_WORKING_DIRECTORY"/pagerank-in"
HDFS_PAGERANK_OUT=$HDFS_WORKING_DIRECTORY"/pagerank-out-"$RAND
HDFS_PAGERANK_PAGES=$HDFS_PAGERANK"/vertex.txt"
HDFS_PAGERANK_LINKS=$HDFS_PAGERANK"/edge.txt"

#TESTJOB
TESTJOB_HOME=$FILES_DIRECTORY"/testjob"
TESTJOB_DATA=$FILES_DIRECTORY"/testjob-data"
HDFS_TESTJOB=$HDFS_WORKING_DIRECTORY"/testjob-in"
HDFS_TESTJOB_OUT=$HDFS_WORKING_DIRECTORY"/testjob-out-"$RAND

#TPCH10
HDFS_TPCH10=$HDFS_TESTJOB
HDFS_TPCH10_OUT=$HDFS_WORKING_DIRECTORY"/tpch10-out-"$RAND

#TPCH3
HDFS_TPCH3=$HDFS_TESTJOB
HDFS_TPCH3_OUT=$HDFS_WORKING_DIRECTORY"/tpch3-out-"$RAND


# overwrite defaults by custom config
if [[ `basename $BASH_SOURCE` == "configDefaults.sh" ]] ; then
	if [[ -e "config.sh" ]]; then
		. ./config.sh
	fi
fi

