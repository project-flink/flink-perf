#!/bin/bash

echo "Stopping Flink"

. ./configDefaults.sh


$FLINK_BUILD_HOME/bin/stop-cluster.sh
