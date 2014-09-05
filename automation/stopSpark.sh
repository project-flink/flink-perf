#!/bin/bash

echo "Stopping Spark"

. ./configDefaults.sh


$SPARK_HOME/sbin/stop-all.sh

