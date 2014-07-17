#!/bin/bash

. ./configDefaults.sh

CMD=$@

echo "Executing the command '$CMD' on all cluster nodes (as defined in flink-conf/slaves)"

if [[ ! -f "flink-conf/slaves" ]] ; then
	echo "There is no flink-conf/slaves file";
	exit 1;
fi

for node in `cat flink-conf/slaves` ; do
	echo "Executing on $node"
	ssh $node -- "$CMD"
done