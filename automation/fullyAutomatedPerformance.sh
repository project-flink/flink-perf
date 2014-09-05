#!/bin/bash

# this script is not as flexible as the other scripts in this directory.
# The overhead of "overgeneralizing" is just too high.
# Author: rmetzger@apache.org

echo "Running fully fledged automated benchmarks"

. ./configDefaults.sh

#export all these variables
set -a

# number of iterations for iterative jobs
ITERATIONS=10
export CORES_PER_MACHINE=16
# for adding new experiments, you also have to edit the loop in runExperiments()
FLINK_EXPERIMENTS="readonly wordcount-wo-combine wordcount kmeans"
#FLINK_EXPERIMENTS=""
SPARK_EXPERIMENTS="readonly wordcount-wo-combine wordcount kmeans"

# Preparation
if [[ ! -e "_config-staging" ]]; then
	mkdir "_config-staging";
fi

benchId="fullbench-"`date +"%d-%m-%y-%H-%M"`
export LOG_dir="benchmarks/"$benchId
mkdir -p "${LOG_dir}"
LOG="$LOG_dir/control"
export TIMES="${LOG_dir}/times"
touch $TIMES

# set output for this file (http://stackoverflow.com/questions/3173131/redirect-copy-of-stdout-to-log-file-from-within-bash-script-itself)
exec > >(tee $LOG)
# also for stdout
exec 2>&1


echo "machines;memory;system;job;time" >> $TIMES
# echo -n "machines;memory;" >> $TIMES
# # write experiments to headers
# MERGED="$FLINK_EXPERIMENTS $SPARK_EXPERIMENTS"
# for EXP in ${MERGED// / } ; do
# 	echo -n "$EXP;" >> $TIMES
# 	echo "EXP=$EXP"
# done
# # write newline
# echo "" >> $TIMES


# arguments
# 1. Number of machines
# 2. Memory in MB per machine.
runExperiments() {
	MACHINES=$1
	MEMORY=$2
	echo "Generate configuration for $MACHINES machines and $MEMORY MB of memory"
	export DOP=`echo $MACHINES*$CORES_PER_MACHINE | bc`
	echo "Set DOP=$DOP"
	head -n $MACHINES flink-conf/slaves > _config-staging/slaves
	cp _config-staging/slaves $FILES_DIRECTORY/flink-build/conf/
	cp _config-staging/slaves $SPARK_HOME/conf/

	# Flink config
	cat flink-conf/flink-conf.yaml > _config-staging/flink-conf.yaml
	# appending ...
	echo "taskmanager.heap.mb: $MEMORY" >> _config-staging/flink-conf.yaml
	cp _config-staging/flink-conf.yaml $FILES_DIRECTORY/flink-build/conf/

	# Spark config
	cat spark-conf/spark-defaults.conf > _config-staging/spark-defaults.conf
	echo "spark.executor.memory            ${MEMORY}m" >> _config-staging/spark-defaults.conf
	echo "spark.default.parallelism            $DOP" >> _config-staging/spark-defaults.conf
	cp _config-staging/spark-defaults.conf $SPARK_HOME/conf/


	# do Flink experiments

	# start Flink
	./startFlink.sh
	echo "waiting for 60 seconds"
	sleep 60

	for EXP in ${FLINK_EXPERIMENTS// / } ; do
		echo "Starting experiment $EXP"
		start=$(date +%s)
		case "$EXP" in
		"readonly")
			./runReadonly.sh &>> $LOG_dir"/flink-$EXP-$MACHINES-$MEMORY-log"
			;;
		"wordcount-wo-combine")
			HDFS_WC_OUT=$HDFS_WORKING_DIRECTORY"/wc-out-$EXP-$MACHINES-$MEMORY-$benchId"
			./runWC-JAPI-withoutCombine.sh &>> $LOG_dir"/flink-$EXP-$MACHINES-$MEMORY-log"
			;;
		"wordcount")
			HDFS_WC_OUT=$HDFS_WORKING_DIRECTORY"/wc-out-$EXP-$MACHINES-$MEMORY-$benchId"
			./runWC-JAPI.sh &>> $LOG_dir"/flink-$EXP-$MACHINES-$MEMORY-log"
			;;
		"kmeans")
			HDFS_KMEANS_OUT=$HDFS_WORKING_DIRECTORY"/kmeans-out-$EXP-$MACHINES-$MEMORY-$benchId"
			./runKMeansPerf.sh &>> $LOG_dir"/flink-$EXP-$MACHINES-$MEMORY-log"
			;;
		# add your experiment here!	
		*)
			echo "Unknown experiment $EXP"
			;;
		esac
		end=$(date +%s)
		expTime=$(($end - $start))
		echo "Experiment took $expTime seconds"
		echo "$MACHINES;$MEMORY;flink;$EXP;$expTime" >> $TIMES
		sleep 2
	done
	./stopFlink.sh
	experimentsFlinkLog="$LOG_dir/flink-$MACHINES-$MEMORY-log/"
	mkdir -p $experimentsFlinkLog
	cp $FILES_DIRECTORY/flink-build/log/* $experimentsFlinkLog
	cp $FILES_DIRECTORY/flink-build/conf/* $experimentsFlinkLog
	rm $FILES_DIRECTORY/flink-build/log/*

	# Start Spark
	./startSpark.sh
	echo "waiting again, this time for Spark"
	sleep 60

	for EXP in ${SPARK_EXPERIMENTS// / } ; do
		echo "Starting spark experiment $EXP"
		start=$(date +%s)
		case "$EXP" in
		"readonly")
			./runSparkReadonly.sh &>> $LOG_dir"/spark-$EXP-$MACHINES-$MEMORY-log"
			;;
		"wordcount-wo-combine")
			HDFS_SPARK_WC_OUT=$HDFS_WORKING_DIRECTORY"/wc-spark-out-$EXP-$MACHINES-$MEMORY-$benchId"
			./runSpark-WC-Grouping-Java.sh &>> $LOG_dir"/spark-$EXP-$MACHINES-$MEMORY-log"
			;;
		"wordcount")
			HDFS_SPARK_WC_OUT=$HDFS_WORKING_DIRECTORY"/wc-spark-out-$EXP-$MACHINES-$MEMORY-$benchId"
			./runSpark-WC-Java.sh &>> $LOG_dir"/spark-$EXP-$MACHINES-$MEMORY-log"
			;;
		"kmeans")
			HDFS_SPARK_KMEANS_OUT=$HDFS_WORKING_DIRECTORY"/kmeans-spark-out-$EXP-$MACHINES-$MEMORY-$benchId"
			./runSparkKMeansPerf-java.sh &>> $LOG_dir"/spark-$EXP-$MACHINES-$MEMORY-log"
			;;
		# add your experiment here!
		*)
			echo "Unknown experiment $EXP"
			;;
		esac
		end=$(date +%s)
		expTime=$(($end - $start))
		echo "Experiment took $expTime seconds"
		echo "$MACHINES;$MEMORY;spark;$EXP;$expTime" >> $TIMES
		sleep 2
	done
	./stopSpark.sh
	experimentsSparkLog="$LOG_dir/spark-$MACHINES-$MEMORY-log/"
	mkdir -p $experimentsSparkLog
	cp $SPARK_HOME/logs/* $experimentsSparkLog
	cp $SPARK_HOME/conf/* $experimentsSparkLog
	rm $SPARK_HOME/logs/*
}

echo "Building Flink, Spark, and Testjobs"
./prepareFlink.sh
./prepareSpark.sh
./prepareTestjob.sh
echo "Testing scaling capabilities from 5 to 25 machines."

# 5 machines, 20GB mem each
runExperiments 5 20000
runExperiments 15 20000
runExperiments 25 20000

echo "Testing memory behavior"

runExperiments 5 5000
runExperiments 5 10000
runExperiments 5 15000

# the 5 / 20000 datapoint is available.

runExperiments 25 5000


echo "Experiments done"

tar czf "$LOG_dir.tgz" $LOG_dir
