FLINK_DIR=/home/robert/flink/build-target

export HADOOP_CONF_DIR=/etc/hadoop/conf

start_job() {
	echo "Starting job on YARN with $1 workers"
	PARA=`echo $1*4 | bc`
	$FLINK_DIR/bin/flink run -m yarn-cluster -yn $1 -yst -yjm 768 -ytm 3072 -ys 4 -yd -p $PARA -c com.github.projectflink.streaming.Throughput /home/robert/flink-perf/flink-jobs/target/flink-jobs-0.1-SNAPSHOT.jar --para 160 --payload 12 --delay 0 --logfreq 1000000 --sourceParallelism 160 --sinkParallelism 160 --latencyFreq 1000000 | tee lastJobOutput
}

kill_on_yarn() {
	KILL=`cat lastJobOutput | grep "yarn application"`
	# get application id by storing the last string
	for word in $KILL
	do
		APPID=$word
	done
	#echo "killing app with '$KILL'"
	exec $KILL
	echo $APPID
}

getLogsFor() {
	echo "Getting logs for $1"
}

start_job 10

# one minute
sleep 60

APPID=`kill_on_yarn`

getLogsFor $APPID
