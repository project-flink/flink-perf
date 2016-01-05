package com.github.projectflink.streaming.utils;

import java.io.Serializable;

public abstract class KafkaPartitioner<T> implements Serializable {
	private static final long serialVersionUID = 1L;

	public abstract void prepare(int parallelInstanceId, int parallelInstances, int[] partitions);

	public abstract int partition(T element);
}
