#!/bin/bash

RUNNAME=$1
FLINK_DIR=/home/robert/flink/build-target
LOG="run-log-$1"

export HADOOP_CONF_DIR=/etc/hadoop/conf
TIMEOUT=100
REPART=1
BUFFERS=2048
SLEEP_FREQ=2
DELAY=1
FT=""
start_job() {
	echo -n "$1;$REPART;$FT;$BUFFERS;$TIMEOUT;" >> $LOG
	echo "Starting job on YARN with $1 workers and a timeout of $TIMEOUT ms"
	PARA=`echo $1*4 | bc`
	CLASS="com.github.projectflink.streaming.ForwardThroughput"
	$FLINK_DIR/bin/flink run -m yarn-cluster -yn $1 -yst -yD taskmanager.network.numberOfBuffers=$BUFFERS -yjm 768 -ytm 3072 -ys 4 -yd -p $PARA -c $CLASS /home/robert/flink-perf/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar --words 2000 --logfreq 20000000 | tee lastJobOutput
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

DURATION=100

experiment 5 $DURATION
experiment 10 $DURATION
experiment 15 $DURATION
experiment 20 $DURATION
experiment 25 $DURATION
experiment 30 $DURATION
experiment 35 $DURATION

