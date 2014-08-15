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

FLINK_DIR=$FILES_DIRECTORY"/flink"

echo "+++ preparing Flink +++"

echo "checking if Flink dir exists ($FLINK_DIR)"
if [[ ! -e $FLINK_DIR ]]; then
	echo "Cloning flink"
	git clone $GIT_REPO flink
fi

echo "Going into Flink dir, fetching and checking out."
cd flink
git remote set-url origin $GIT_REPO
git fetch origin
git checkout origin/$GIT_BRANCH

echo "building flink"
$MVN_BIN clean install -DskipTests -Dmaven.javadoc.skip=true $CUSTOM_FLINK_MVN

cd $FILES_DIRECTORY

if [[ $YARN == "true" ]]; then
	rm -r *yarn.tar.gz
	cp -r flink/flink-dist/target/*yarn.tar.gz .
	tar xzf *yarn.tar.gz
	mkdir flink-build
	cd flink-build
	rm -rf *
	cd ..
	mv flink-yarn-*/* flink-build/
else
	rm -rf flink-build
	mkdir flink-build
	cp -r flink/flink-dist/target/flink-*-bin/flink-*/* flink-build
	cp flink/flink-tests/target/*.jar flink-build/examples
fi

cd $INITIAL

./updateConfig.sh
