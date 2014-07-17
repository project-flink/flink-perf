#!/bin/bash

echo "Preparing the work environment"

. ./configDefaults.sh

echo "GIT_REPO=$GIT_REPO"

INITIAL=`pwd`
echo "checking if FILES_DIRECTORY exists"
if [[ ! -e $FILES_DIRECTORY ]]; then
	mkdir $FILES_DIRECTORY;
fi

cd $FILES_DIRECTORY

STRATO_DIR=$FILES_DIRECTORY"/stratosphere"

echo "+++ preparing Stratosphere +++"

echo "checking if strato dir exists ($STRATO_DIR)"
if [[ ! -e $STRATO_DIR ]]; then
	echo "Cloning stratosphere"
	git clone $GIT_REPO stratosphere
fi

echo "Going into strato dir, fetching and checking out."
cd stratosphere
git remote set-url origin $GIT_REPO
git fetch origin
git checkout origin/$GIT_BRANCH

echo "building stratosphere"
$MVN_BIN clean install -DskipTests -Dmaven.javadoc.skip=true $CUSTOM_STRATOSPHERE_MVN

cd $FILES_DIRECTORY

if [[ $YARN == "true" ]]; then
	rm -r *yarn.tar.gz
	cp -r stratosphere/flink-dist/target/*yarn.tar.gz .
	tar xzf *yarn.tar.gz
	mkdir flink-build
	cd flink-build
	rm -rf *
	cd ..
	mv flink-yarn-*/* flink-build/
else
	rm -rf flink-build
	mkdir flink-build
	cp -r stratosphere/flink-dist/target/*-dist-*-bin/flink-*/* flink-build
	cp stratosphere/flink-tests/target/*.jar flink-build/examples
fi

cd $INITIAL

./updateConfig.sh
