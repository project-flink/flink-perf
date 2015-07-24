package com.github.projectflink.streaming.utils;

import java.io.Serializable;

public abstract class KafkaPartitioner<T> implements Serializable{
	public abstract void prepare(int parallelInstanceId, int parallelInstances, int[] partitions);

	public abstract int partition(T element);
}
