#!/bin/bash

echo "Starting Flink"

. ./configDefaults.sh

if [[ $YARN == "true" ]]; then

	#tmux new -d -s 
	$FLINK_BUILD_HOME/bin/yarn-session.sh $YARN_SESSION_CONF
	echo "Starting a YARN session in background, with the following config: $YARN_SESSION_CONF. Logging to yarn_session_log"
else
	$FLINK_BUILD_HOME/bin/start-cluster.sh
fi
