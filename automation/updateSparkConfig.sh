#!/bin/bash


echo "Updating Spark configuration. Remember to restart"

. ./configDefaults.sh

for file in spark-conf/* ; do
	filename=$(basename "$file")
	extension="${filename##*.}"
	if [[ "$extension" != "template" ]]; then
		echo "copying $file to $SPARK_HOME/conf/"
		cp $file $SPARK_HOME/conf/
	fi
done
