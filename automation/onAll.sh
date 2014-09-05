#!/bin/bash

. ./configDefaults.sh
. ./utils.sh

HOSTLIST=flink-conf/slaves

echo "Executing command $@ on all nodes."

GOON=true
while $GOON
do
    read line || GOON=false
    if [ -n "$line" ]; then
        HOST=$line
        echo "[$HOST]"
        ssh -n $HOST -- $@
    fi
done < "$HOSTLIST"


