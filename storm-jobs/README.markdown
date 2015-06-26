# Storm Jobs


## Run throughput on storm cluster

storm jar storm-jobs-0.1-SNAPSHOT.jar  experiments.Throughput --para 3 --payload 12 --delay 10 --logfreq 10000 --sourceParallelism 3 --sinkParallelism 3 
