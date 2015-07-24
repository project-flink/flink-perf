package com.github.projectflink.streaming.utils;

/**
 * A partitioner ensuring that each internal Flink partition ends up in one Kafka partition.
 *
 * Note, one Kafka partition can contain multiple Flink partitions.
 *
 * Cases:
 * 	# More Flink partitions than kafka partitions
 *
 * 		Flink Sinks:		Kafka Partitions
 * 			1	---------------->	1
 * 			2   --------------/
 * 			3   -------------/
 * 			4	------------/
 *
 * 	--> Some (or all) kafka partitions contain the output of more than one flink partition
 *
 *# Fewer Flink partitions than Kafka
 *
 * 		Flink Sinks:		Kafka Partitions
 * 			1	---------------->	1
 * 			2	---------------->	2
 * 									3
 * 									4
 * 									5
 *
 *  --> Not all Kafka partitions contain data
 *  To avoid such an unbalanced partitioning, use a round-robin kafka partitioner. (note that this will
 *  cause a lot of network connections between all the Flink instances and all the Kafka brokers
 *
 *
 * @param <T>
 */
public class FixedPartitioning<T> extends KafkaPartitioner<T> {

	int targetPartition = -1;

	@Override
	public void prepare(int parallelInstanceId, int parallelInstances, int[] partitions) {
		int p = 0;
		for(int i = 0; i < parallelInstances; i++) {
			if(i == parallelInstanceId) {
				targetPartition = partitions[p];
				return;
			}
			if(++p == partitions.length) {
				p = 0;
			}
		}
	}

	@Override
	public int partition(T element) {
		if(targetPartition == -1) {
			throw new RuntimeException("The partitioner has not been initialized properly");
		}
		return targetPartition;
	}
}
