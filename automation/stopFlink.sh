#!/bin/bash

echo "Stopping Flink"

. ./configDefaults.sh

if [[ $YARN == "true" ]]; then
	# I hope you are not using tmux. THe ideal approach would be to store and kill the pid.
	killall tmux
else
	$FLINK_BUILD_HOME/bin/stop-cluster.sh
fi

