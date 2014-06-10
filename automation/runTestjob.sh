#!/bin/bash

echo "Running testjob"

. ./configDefaults.sh

# job arguments example
# hdfs:///user/twalthr/customer.tbl 
# hdfs:///user/twalthr/lineitem.tbl 
# hdfs:///user/twalthr/nation.tbl 
# hdfs:///user/twalthr/orders.tbl 
# hdfs:///user/twalthr/region.tbl 
# hdfs:///user/twalthr/ordersAvro.avro 
# “seqfile” 
# hdfs:///user/twalthr/out 
# 1500

# Jobargs documentation

# customer = args[0];
# lineitem = args[1];
# nation = args[2];
# orders = args[3];
# region = args[4];
# orderAvroFile = args[5];
# sequenceFileInput = args[6];
# outputTableDirectory = args[7];
# maxBulkIterations = Integer.valueOf(args[8]);

echo "running testjob"
$STRATOSPHERE_BUILD_HOME"/bin/stratosphere" run -v -j $TESTJOB_HOME"/target/testjob-*.jar" \
	-c eu.stratosphere.test.testPlan.LargeTestPlan \
	$HDFS_TESTJOB/customer.tbl \
	$HDFS_TESTJOB/lineitem.tbl \
	$HDFS_TESTJOB/nation.tbl \
	$HDFS_TESTJOB/orders.tbl \
	$HDFS_TESTJOB/region.tbl \
	$HDFS_TESTJOB/orders.avro \
	sequenceFileTestIsCurrentlyNotActivated \
	$HDFS_TESTJOB_OUT \
	10000 \
	$DOP


