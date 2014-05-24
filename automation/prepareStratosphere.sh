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
	git clone $GIT_REPO
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
	cp -r stratosphere/stratosphere-dist/target/*yarn.tar.gz .
	tar xzf *yarn.tar.gz
	mv stratosphere-yarn-* stratosphere-build
else
	rm -rf stratosphere-build
	mkdir stratosphere-build
	cp -r stratosphere/stratosphere-dist/target/stratosphere-dist-*-bin/stratosphere-*/* stratosphere-build
	cp stratosphere/stratosphere-tests/target/*.jar stratosphere-build/examples
fi

cd $INITIAL

./updateConfig.sh
