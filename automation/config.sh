
# This is a simple bash file containing configuration values as variables

# the repo must be called stratosphere
GIT_REPO=https://github.com/stratosphere/stratosphere.git
GIT_BRANCH=variousFixes

# the repo must be called testjob
TESTJOB_REPO=https://github.com/rmetzger/testjob.git
TESTJOB_BRANCH=testAutomation

YARN=false
YARN_SESSION_CONF="-n 2 -jm 500 -tm 500"

# has to be a absolute path!
FILES_DIRECTORY=`pwd`/workdir
HDFS_WORKING_DIRECTORY=file:///tmp/stratosphere-tests
RAND=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 12 | head -n 1`

MVN_BIN=mvn

#custom mvn flags (most likely -Dhadoop.profile=2 )
CUSTOM_STRATOSPHERE_MVN=""
if [[ $YARN == "true" ]]; then
	CUSTOM_STRATOSPHERE_MVN=" -Dhadoop.profile=2 "
fi

HADOOP_BIN="/home/robert/Projekte/ozone/work/hadoop-2.3.0/bin/hadoop"

# General Stuff
DOP=8

# Wordcount stuff.
FILES_WC_GEN=$FILES_DIRECTORY"/wc-data/generated-wc.txt"
HDFS_WC=$HDFS_WORKING_DIRECTORY"/wc-in"
HDFS_WC_OUT=$HDFS_WORKING_DIRECTORY"/wc-out-"$RAND

# Directories
STRATOSPHERE_BUILD_HOME=$FILES_DIRECTORY"/stratosphere-build"
TESTJOB_HOME=$FILES_DIRECTORY"/testjob"

TESTJOB_DATA=$FILES_DIRECTORY"/testjob-data"
HDFS_TESTJOB=$HDFS_WORKING_DIRECTORY"/testjob-in"
HDFS_TESTJOB_OUT=$HDFS_WORKING_DIRECTORY"/testjob-out-"$RAND
