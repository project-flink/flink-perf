#!/bin/bash

echo "Stopping Stratosphere"

. ./configDefaults.sh


$STRATOSPHERE_BUILD_HOME/bin/stop-cluster.sh
