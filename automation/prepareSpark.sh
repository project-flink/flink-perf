#!/bin/bash

echo "Preparing the work environment"

. ./configDefaults.sh


INITIAL=`pwd`
echo "checking if FILES_DIRECTORY exists"
if [[ ! -e $FILES_DIRECTORY ]]; then
	mkdir $FILES_DIRECTORY;
fi

cd $FILES_DIRECTORY

SPARK_DIR=$FILES_DIRECTORY"/spark"

echo "+++ preparing Spark +++"

echo "checking if Spark dir exists (SPARK_DIR=$SPARK_DIR)"
if [[ ! -e $SPARK_DIR ]]; then
	echo "Cloning spark"
	git clone $SPARK_GIT_REPO spark
fi

echo "Going into Spark dir, fetching and checking out."
cd spark
git remote set-url origin $SPARK_GIT_REPO
git fetch origin
git checkout origin/$SPARK_GIT_BRANCH


echo "building spark"
#$MVN_BIN clean install -DskipTests -Dmaven.javadoc.skip=true $CUSTOM_FLINK_MVN
eval "sbt/sbt -Dhadoop.version=2.2.0 -Pyarn assembly"
cd $FILES_DIRECTORY



cd $INITIAL

./updateSparkConfig.sh
