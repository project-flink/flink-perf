#!/bin/bash


echo "Updating configuration. Remember to restart"

. ./configDefaults.sh

for file in flink-conf/* ; do
	filename=$(basename "$file")
	extension="${filename##*.}"
	if [[ "$extension" != "template" ]]; then
		echo "copying $file to $FILES_DIRECTORY/flink-build/conf/"
		cp $file $FILES_DIRECTORY/flink-build/conf/
	fi
done
