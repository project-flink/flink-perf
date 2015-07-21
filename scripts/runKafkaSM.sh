#!/bin/bash

RUNNAME=$1
FLINK_DIR=/home/robert/flink/build-target
LOG="kafka-gen-log-$1"
BROKERS="robert-streaming-m.c.astral-sorter-757.internal:6667,robert-streaming-w-0.c.astral-sorter-757.internal:6667,robert-streaming-w-1.c.astral-sorter-757.internal:6667,robert-streaming-w-10.c.astral-sorter-757.internal:6667,robert-streaming-w-11.c.astral-sorter-757.internal:6667,robert-streaming-w-12.c.astral-sorter-757.internal:6667,robert-streaming-w-13.c.astral-sorter-757.internal:6667,robert-streaming-w-14.c.astral-sorter-757.internal:6667,robert-streaming-w-15.c.astral-sorter-757.internal:6667,robert-streaming-w-16.c.astral-sorter-757.internal:6667,robert-streaming-w-17.c.astral-sorter-757.internal:6667,robert-streaming-w-18.c.astral-sorter-757.internal:6667,robert-streaming-w-19.c.astral-sorter-757.internal:6667,robert-streaming-w-2.c.astral-sorter-757.internal:6667,robert-streaming-w-21.c.astral-sorter-757.internal:6667,robert-streaming-w-22.c.astral-sorter-757.internal:6667,robert-streaming-w-23.c.astral-sorter-757.internal:6667,robert-streaming-w-24.c.astral-sorter-757.internal:6667,robert-streaming-w-25.c.astral-sorter-757.internal:6667,robert-streaming-w-26.c.astral-sorter-757.internal:6667,robert-streaming-w-27.c.astral-sorter-757.internal:6667,robert-streaming-w-28.c.astral-sorter-757.internal:6667,robert-streaming-w-29.c.astral-sorter-757.internal:6667,robert-streaming-w-30.c.astral-sorter-757.internal:6667,robert-streaming-w-31.c.astral-sorter-757.internal:6667,robert-streaming-w-32.c.astral-sorter-757.internal:6667,robert-streaming-w-34.c.astral-sorter-757.internal:6667,robert-streaming-w-35.c.astral-sorter-757.internal:6667,robert-streaming-w-36.c.astral-sorter-757.internal:6667,robert-streaming-w-37.c.astral-sorter-757.internal:6667,robert-streaming-w-38.c.astral-sorter-757.internal:6667,robert-streaming-w-39.c.astral-sorter-757.internal:6667,robert-streaming-w-4.c.astral-sorter-757.internal:6667,robert-streaming-w-5.c.astral-sorter-757.internal:6667,robert-streaming-w-6.c.astral-sorter-757.internal:6667,robert-streaming-w-7.c.astral-sorter-757.internal:6667,robert-streaming-w-8.c.astral-sorter-757.internal:6667,robert-streaming-w-9.c.astral-sorter-757.internal:6667"

export HADOOP_CONF_DIR=/etc/hadoop/conf

start_job() {
	echo -n "$1;$REPART;$FT;$BUFFERS;$TIMEOUT;" >> $LOG
	echo "Starting job on YARN with $1 workers and a timeout of $TIMEOUT ms"
	PARA=`echo $1*1 | bc`
	CLASS="com.dataartisans.flink.example.eventpattern.StreamingDemo"
	$FLINK_DIR/bin/flink run -m yarn-cluster -yn $1 -yst -yD "execution-retries.delay=30 s" -yjm 768 -ytm 3072 -ys 1 -yd -p $PARA -c $CLASS \
	/home/robert/flink-perf/streaming-state-machine/target/streaming-state-demo-1.0-SNAPSHOT.jar --ft 10000 --group.id ex-4-grp-1 \
	--rebalance.backoff.ms 4000 --rebalance.max.retries 10 \
         --auto.offset.reset earliest --logFreq 500000 --topic experiment-5 --bootstrap.servers $BROKERS --error-topic error-topic  \
	--zookeeper.connect robert-streaming-m.c.astral-sorter-757.internal:2181,robert-streaming-w-0.c.astral-sorter-757.internal:2181,robert-streaming-w-1.c.astral-sorter-757.internal:2181 | tee lastJobOutput
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


DURATION=3600

experiment 30 $DURATION





