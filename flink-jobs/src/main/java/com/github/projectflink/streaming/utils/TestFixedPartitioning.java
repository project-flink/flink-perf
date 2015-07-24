package com.github.projectflink.streaming.utils;

import org.junit.Assert;
import org.junit.Test;

public class TestFixedPartitioning {


	/**
	 *   		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2   --------------/
	 * 			3   -------------/
	 * 			4	------------/
	 */
	@Test
	public void testMoreFlinkThanBrokers() {
		FixedPartitioning<String> part = new FixedPartitioning<String>();

		int[] partitions = new int[]{0};

		part.prepare(0, 4, partitions);
		Assert.assertEquals(0, part.partition("abc1"));

		part.prepare(1, 4, partitions);
		Assert.assertEquals(0, part.partition("abc2"));

		part.prepare(2, 4, partitions);
		Assert.assertEquals(0, part.partition("abc3"));
		Assert.assertEquals(0, part.partition("abc3")); // check if it is changing ;)

		part.prepare(3, 4, partitions);
		Assert.assertEquals(0, part.partition("abc4"));
	}

	/*
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	---------------->	1
	 * 			2	---------------->	2
	 * 									3
	 * 									4
	 * 									5
	 */
	@Test
	public void testFewerPartitions() {
		FixedPartitioning<String> part = new FixedPartitioning<String>();

		int[] partitions = new int[]{0, 1, 2, 3, 4};
		part.prepare(0, 2, partitions);
		Assert.assertEquals(0, part.partition("abc1"));
		Assert.assertEquals(0, part.partition("abc1"));

		part.prepare(1, 2, partitions);
		Assert.assertEquals(1, part.partition("abc1"));
		Assert.assertEquals(1, part.partition("abc1"));
	}

	/*
	 * 		Flink Sinks:		Kafka Partitions
	 * 			1	------------>--->	1
	 * 			2	-----------/----> 	2
	 * 			3	----------/
	 */
	@Test
	public void testMixedCase() {
		FixedPartitioning<String> part = new FixedPartitioning<String>();
		int[] partitions = new int[]{0,1};

		part.prepare(0, 3, partitions);
		Assert.assertEquals(0, part.partition("abc1"));

		part.prepare(1, 3, partitions);
		Assert.assertEquals(1, part.partition("abc1"));

		part.prepare(2, 3, partitions);
		Assert.assertEquals(0, part.partition("abc1"));

	}

	}
