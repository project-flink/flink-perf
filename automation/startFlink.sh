#!/bin/bash

echo "Starting Flink"

. ./configDefaults.sh

if [[ $YARN == "true" ]]; then
	$FLINK_BUILD_HOME/bin/yarn-session.sh $YARN_SESSION_CONF
else
	$FLINK_BUILD_HOME/bin/start-cluster.sh
fi
