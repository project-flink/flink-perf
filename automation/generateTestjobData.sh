#!/bin/bash

. ./configDefaults.sh

SCALE=$1
if [[ -z "$SCALE" ]]; then
	SCALE=1
	echo "No argument passed. Scale factor set to $SCALE"
fi

echo "Generating Data that is required for running the tasks"


cd $FILES_DIRECTORY
mkdir -p testjob-data
cd testjob-data

if [[ ! -d "tpch_2_16_0" ]]; then
	echo "TPCH does not seem to be installed. Let me do that for you"
	wget http://www.tpc.org/tpch/spec/tpch_2_16_0.zip
	unzip tpch_2_16_0.zip
	cd tpch_2_16_0
	cd dbgen
	mv makefile.suite Makefile

	# set Makefile configuration values
	# $OS defined in config.sh
	if [ "$OS" == 'Linux' ]; then
		# set Makefile configuration values
		sed -i 's/CC      =/CC      = cc/g' Makefile
		sed -i 's/DATABASE=/DATABASE= SQLSERVER/g' Makefile
		sed -i 's/MACHINE =/MACHINE = LINUX/g' Makefile
		sed -i 's/WORKLOAD =/WORKLOAD = TPCH/g' Makefile
	elif [ "$OS" == 'Darwin' ]; then
		sed -i "" 's/CC      =/CC      = cc/g' Makefile
		sed -i "" 's/DATABASE=/DATABASE= SQLSERVER/g' Makefile
		sed -i "" 's/MACHINE =/MACHINE = LINUX/g' Makefile
		sed -i "" 's/WORKLOAD =/WORKLOAD = TPCH/g' Makefile
		sed -i "" 's/<malloc.h>/<sys\/malloc.h>/g' bm_utils.c
		sed -i "" 's/<malloc.h>/<sys\/malloc.h>/g' varsub.c
	else
		echo "System $OS is not supported"
	fi

	make
fi

cd $FILES_DIRECTORY
cd testjob-data
cd tpch_2_16_0
cd dbgen

# -f force -s scale factor
./dbgen -f -s $SCALE

cd $FILES_DIRECTORY
cd testjob-data
mv tpch_2_16_0/dbgen/*.tbl .

echo "TPCH generator done"

echo "checking if we need to generate avro files"

if [[ ! -e "order.avro" ]]; then
	echo "there is not order.avro file. Creating it"
	echo "cmd java -cp $TESTJOB_HOME/target/flink-perf-0.1-SNAPSHOT.jar com.github.projectflink.testPlan.LargeTestPlan orders.tbl orders.avro"
	java -cp $TESTJOB_HOME"/target/flink-perf-0.1-SNAPSHOT.jar" com.github.projectflink.testPlan.LargeTestPlan orders.tbl orders.avro
	echo "done"
fi

