#!/bin/bash

RUNNAME=$1
STORM_JOB_TARGET=/home/robert/flink-perf/storm-jobs/target
LOG="run-log-storm-$1"


REPART=1
DELAY=1
#sleep for 1 ms every second record
SLEEP_FREQ=2
FT=""

export HADOOP_CONF_DIR=/etc/hadoop/conf
JOB_ID=""
start_job() {
	JOB_ID=`cat /dev/urandom | tr -dc 'a-zA-Z0-9' | fold -w 32 | head -n 1`
	echo -n "$1;$JOB_ID;$REPART;$FT;" >> $LOG
	echo "Starting job on Storm with $1 workers, repart $REPART"
	PARA=`echo $1*4 | bc`
	# experiments.Throughput
	storm jar $STORM_JOB_TARGET/storm-jobs-0.1-SNAPSHOT.jar experiments.ThroughputHostsTracking --delay $DELAY $FT --sleepFreq $SLEEP_FREQ --repartitions $REPART --para $1 --name $JOB_ID --payload 12 --logfreq 10000 --sourceParallelism $PARA --sinkParallelism $PARA --latencyFreq 2000 | tee lastJobOutput
}

append() {
	echo -n "$1;" >> $LOG
}

duration() {
	sleep $1
	append "$1"
}

kill_on_storm() {
	storm kill throughput-$JOB_ID
	sleep 30
}
FILENAME=""
getLogsFor() {
	#cd /var/log/storm
	#FILENAME=`ls -l | grep $JOB_ID | head -n1 | rev |  cut -d" " -f1 | rev`
	#cd -
	for i in $(seq 0 39);
	do
		echo "Getting log file from machine $i"
		scp "robert-streaming-w-$i":/var/log/storm/*$JOB_ID* logs/robert-streaming-w-$i-$JOB_ID
	done
	cat logs/robert-streaming-w-*-$JOB_ID > logs/aggregated-$JOB_ID
	#echo $FILENAME
}

analyzeLogs() {
	java -cp /home/robert/flink-perf/perf-common/target/perf-common-0.1-SNAPSHOT-jar-with-dependencies.jar com.github.projectflink.common.AnalyzeTool logs/aggregated-$JOB_ID >> $LOG
}

function experiment() {
	start_job $1
	duration $2
	kill_on_storm

	getLogsFor
	analyzeLogs
}

echo "machines;job-id;duration-sec;lat-mean;lat-median;lat-90percentile;lat-95percentile;lat-99percentile;throughput-mean;throughput-max;latencies;throughputs" >> $LOG

REPART=4
DURATION=180
#experiment 30 $DURATION

SLEEP_FREQ=1

FT=" --ft "
experiment 30 $DURATION

exit

DURATION=900

#experiment 10 $DURATION
#experiment 10 $DURATION
#experiment 10 $DURATION

#experiment 20 $DURATION
#experiment 20 $DURATION
#experiment 20 $DURATION

#experiment 30 $DURATION
#experiment 30 $DURATION
#experiment 30 $DURATION


REPART=2
experiment 30 $DURATION
experiment 30 $DURATION
experiment 30 $DURATION

REPART=4
experiment 30 $DURATION
experiment 30 $DURATION
experiment 30 $DURATION

FT=" --ft "


REPART=2
experiment 30 $DURATION
experiment 30 $DURATION
experiment 30 $DURATION

REPART=4
experiment 30 $DURATION
experiment 30 $DURATION
experiment 30 $DURATION




