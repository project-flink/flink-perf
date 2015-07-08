#!/bin/bash

RUNNAME=$1
FLINK_DIR=/home/robert/flink/build-target
LOG="run-log-$1"

export HADOOP_CONF_DIR=/etc/hadoop/conf

start_job() {
	echo -n "$1;" >> $LOG
	echo "Starting job on YARN with $1 workers"
	PARA=`echo $1*4 | bc`
	$FLINK_DIR/bin/flink run -m yarn-cluster -yn $1 -yst -yjm 768 -yD taskmanager.network.numberOfBuffers=8192 -ytm 3072 -ys 4 -yd -p $PARA -c com.github.projectflink.streaming.Throughput /home/robert/flink-perf/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar --para 160 --payload 12 --delay 0 --logfreq 1000000 --sourceParallelism 160 --sinkParallelism 160 --latencyFreq 1000000 | tee lastJobOutput
}

append() {
	echo -n "$1;" >> $LOG
}

duration() {
	sleep $1
	append "$1"
}

kill_on_yarn() {
	KILL=`cat lastJobOutput | grep "yarn application"`
	# get application id by storing the last string
	for word in $KILL
	do
		AID=$word
	done
	append $AID
	echo $AID
	exec $KILL > /dev/null
}

getLogsFor() {
	sleep 30
	yarn logs -applicationId $1 > logs/$1
}
analyzeLogs() {
	java -cp /home/robert/flink-perf/perf-common/target/perf-common-0.1-SNAPSHOT-jar-with-dependencies.jar com.github.projectflink.common.AnalyzeTool logs/$1 >> $LOG
}

function experiment() {
	start_job $1
	duration $2
	APPID=`kill_on_yarn`

	getLogsFor $APPID
	analyzeLogs $APPID
}

echo "machines;duration-sec;yarnAppId;lat-mean;lat-median;lat-90percentile;throughput-mean;throughput-max;latencies;throughputs" >> $LOG

DURATION=1800

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





