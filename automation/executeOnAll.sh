#!/bin/bash

. ./configDefaults.sh

CMD=$@

echo "Executing the command '$CMD' on all cluster nodes (as defined in str-conf/slaves)"

if [[ ! -f "str-conf/slaves" ]] ; then
	echo "There is no str-conf/slaves file";
	exit 1;
fi

for node in `cat str-conf/slaves` ; do
	echo "Executing on $node"
	ssh $node -- "$CMD"
done