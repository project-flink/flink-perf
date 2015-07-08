#!/bin/bash

RUNNAME=$1
STORM_JOB_TARGET=/home/robert/flink-perf/storm-jobs/target
LOG="run-log-storm-$1"

export HADOOP_CONF_DIR=/etc/hadoop/conf

start_job() {
	echo -n "$1;" >> $LOG
	echo "Starting job on Storm with $1 workers"
	PARA=`echo $1*4 | bc`
	storm jar $STORM_JOB_TARGET/storm-jobs-0.1-SNAPSHOT.jar experiments.Throughput --para $1 --payload 12 --delay 0 --logfreq 1000000 --sourceParallelism $PARA --sinkParallelism $PARA --latencyFreq 1000000 | tee lastJobOutput
}

append() {
	echo -n "$1;" >> $LOG
}

duration() {
	sleep $1
	append "$1"
}

kill_on_storm() {
	storm kill throughput
	sleep 30
}
FILENAME=""
getLogsFor() {
	cd /var/log/storm
	FILENAME=`ls -l -t throughput-* | head -n1 | rev |  cut -d" " -f1 | rev`
	cd -
	for i in $(seq 0 39);
	do
		echo "Getting log file from machine $i"
		scp "robert-streaming-w-$i":/var/log/storm/$FILENAME logs/robert-streaming-w-$i-$FILENAME
	done
	cat logs/robert-streaming-w-*-$FILENAME > logs/aggregated-$FILENAME
	#echo $FILENAME
}

analyzeLogs() {
	java -cp /home/robert/flink-perf/perf-common/target/perf-common-0.1-SNAPSHOT-jar-with-dependencies.jar com.github.projectflink.common.AnalyzeTool logs/aggregated-$1 >> $LOG
}

function experiment() {
#	start_job $1
#	duration $2
#	kill_on_storm

	getLogsFor
	echo "FileName=$FILENAME"
	analyzeLogs $FILENAME
}

echo "machines;duration-sec;yarnAppId;lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs" >> $LOG

DURATION=60

#experiment 10 $DURATION
#experiment 10 $DURATION
#experiment 10 $DURATION

#experiment 20 $DURATION
#experiment 20 $DURATION
#experiment 20 $DURATION

#experiment 30 $DURATION
#experiment 30 $DURATION
#experiment 30 $DURATION

experiment 40 $DURATION
#experiment 40 $DURATION
#experiment 40 $DURATION





