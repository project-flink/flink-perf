#!/bin/bash

echo "Starting Spark"

. ./configDefaults.sh


$SPARK_HOME/sbin/stop-all.sh

