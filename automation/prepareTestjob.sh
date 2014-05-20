#!/bin/bash

echo "Preparing the work environment"

. ./configDefaults.sh

echo "checking if FILES_DIRECTORY exists"
if [[ ! -e $FILES_DIRECTORY ]]; then
	mkdir $FILES_DIRECTORY;
fi

cd $FILES_DIRECTORY


echo "+++ preparing the testjob +++"

cd $FILES_DIRECTORY

TESTJOB_DIR=$FILES_DIRECTORY"/testjob"
echo "checking if testjob dir exists ($TESTJOB_DIR)"
if [[ ! -e $TESTJOB_DIR ]]; then
	echo "Cloning testjob"
	git clone $TESTJOB_REPO
fi
cd $TESTJOB_DIR
git fetch origin
git checkout origin/$TESTJOB_BRANCH

echo "building testjob"
$MVN_BIN clean package $CUSTOM_TESTJOB_MVN

cd $FILES_DIRECTORY
