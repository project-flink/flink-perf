# Storm Jobs


## Run throughput on storm cluster

storm jar storm-jobs-0.1-SNAPSHOT.jar  experiments.Throughput --para 3 --payload 12 --delay 10 --logfreq 10000 --sourceParallelism 3 --sinkParallelism 3 



## Measure throughput.

Cluster: 3 machines.

storm jar storm-jobs-0.1-SNAPSHOT.jar  experiments.Throughput --para 6 --payload 12 --delay 0 --logfreq 100000 --sourceParallelism 6 --sinkParallelism 6

measured: 130k/second (3.12 MB/s)


start flink on yarn

./bin/yarn-session.sh -n 6 -s 1 -st -tm 768 -d
