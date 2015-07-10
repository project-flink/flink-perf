package com.github.projectflink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;

public class Latency {
	private static final Logger LOG = LoggerFactory.getLogger(Latency.class);

	public static class T extends Tuple4<Long, String, Long, byte[]>{
		public T() {

		}
		public T(Long value0, String value1, Long value2, byte[] value3) {
			super(value0, value1, value2, value3);
		}
	}

	public static class Source extends RichParallelSourceFunction<T> implements Checkpointed<Long> {

		final ParameterTool pt;
		byte[] payload;
		long id = 0;
		boolean running = true;
		long time = 0;

		public Source(ParameterTool pt) {
			this.pt = pt;
			payload = new byte[pt.getInt("payload")];
		}

		@Override
		public void run(SourceContext<T> sourceContext) throws Exception {
			int delay = pt.getInt("delay");
			int latFreq = pt.getInt("latencyFreq");
			int nextlat = 1000;
			String host = InetAddress.getLocalHost().getHostName();
			LOG.info("Starting data source on host "+host);
			while(running) {
				if (delay > 0) {
					try {
						Thread.sleep(delay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				// move the ID for the latency so that we distribute it among the machines.
				if(id % latFreq == nextlat) {
					time = System.currentTimeMillis();
				//	LOG.info("Sending latency "+time+" from host "+host+" with id "+id);
					if(--nextlat <= 0) {
						nextlat = 1000;
					}
				}

				sourceContext.collect(new T(id++, host, time, payload));
				time = 0;
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public Long snapshotState(long l, long l1) throws Exception {
			return id;
		}

		@Override
		public void restoreState(Long aLong) {
			this.id = aLong;
		}
	}


	public static void main(String[] args) throws Exception {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		see.getConfig().setGlobalJobParameters(pt);

		DataStreamSource<T> in = see.addSource(new Source(pt));
		in.partitionByHash(0).map(new MapFunction<T, T>() {
			@Override
			public T map(T longIntegerLongTuple4) throws Exception {
				return longIntegerLongTuple4;
			}
		}).partitionByHash(0).flatMap(new FlatMapFunction<T, Integer>() {
			public String host = null;
			long received = 0;
			long start = 0;
			long logfreq = pt.getInt("logfreq");
			long lastLog = -1;
			long lastElements = 0;

			@Override
			public void flatMap(T element, Collector<Integer> collector) throws Exception {
				if(host == null) {
					host = InetAddress.getLocalHost().getHostName();
				}
				if (start == 0) {
					start = System.currentTimeMillis();
				}
				received++;
				if (received % logfreq == 0) {
					// throughput over entire time
					long now = System.currentTimeMillis();

					// throughput for the last "logfreq" elements
					if(lastLog == -1) {
						// init (the first)
						lastLog = now;
						lastElements = received;
					} else {
						long timeDiff = now - lastLog;
						long elementDiff = received - lastElements;
						double ex = (1000/(double)timeDiff);
						LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core", timeDiff, elementDiff, elementDiff*ex);
						// reinit
						lastLog = now;
						lastElements = received;
					}
				}
				if (element.f2 != 0 && element.f1.equals(host)) {
					long lat = System.currentTimeMillis() - element.f2;
					LOG.info("Latency "+lat+" ms from machine " + element.f1+" on host "+host+" with element "+element.f0);
				}
			}
		});

		see.execute();
	}
}
