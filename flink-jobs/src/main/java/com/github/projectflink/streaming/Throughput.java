package com.github.projectflink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Throughput {

	private static final Logger LOG = LoggerFactory.getLogger(Throughput.class);

	public static class Type extends Tuple4<Long, // sequence number from generator instance
			Integer,  // host ID on GCE
			Long, // timestamp
			byte[] // some payload
			> {
		public Type(Long value0, Integer value1, Long value2, byte[] value3) {
			super(value0, value1, value2, value3);
		}

		public Type() {
		}

		public Type copy() {
			return new Type(this.f0, this.f1, this.f2, this.f3);
		}
	}

	@Test
	public void testMaster() {
		Assert.assertEquals(-1, convertHostnameToInt("robert-streaming-m.c.astral-sorter-757.internal"));
		Assert.assertEquals(30, convertHostnameToInt("robert-streaming-w-30.c.astral-sorter-757.internal"));
		Assert.assertEquals(3, convertHostnameToInt("robert-streaming-w-3.c.astral-sorter-757.internal"));
		Assert.assertEquals(11, convertHostnameToInt("robert-streaming-w-11"));
		Assert.assertEquals(1, convertHostnameToInt("robert-streaming-w-1"));

	}

	public static int convertHostnameToInt(String host) {
		// allow me to develop locally
		if(host.equals("robert-da")) return 0;

		Pattern p = Pattern.compile("robert-streaming-(m|w-([0-9]+))(.c.astral-sorter-757.internal)?");
		Matcher m = p.matcher(host);
		if(m.matches()){
			if(m.group(1).startsWith("m")) {
				return -1; // that's the master machine
			} else {
				String id = m.group(2);
				return Integer.valueOf(id);
			}
		} else {
			throw new RuntimeException("Unable to convert host "+host);
		}
	}

	public static class Source extends RichParallelSourceFunction<Type> implements Checkpointed<Long> {

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
		public void run(SourceContext<Type> sourceContext) throws Exception {
			int delay = pt.getInt("delay");
			int latFreq = pt.getInt("latencyFreq");
			int nextlat = 1000;
			int sleepFreq = pt.getInt("sleepFreq");
		//	String host = InetAddress.getLocalHost().getHostName();
			int host = convertHostnameToInt(InetAddress.getLocalHost().getHostName());

			while(running) {
				if(delay > 0) {
					if(id % sleepFreq == 0) {
						try { Thread.sleep(delay); } catch (InterruptedException e) { e.printStackTrace();}
					}
				}
				// move the ID for the latency so that we distribute it among the machines.
				if(id % latFreq == nextlat) {
					time = System.currentTimeMillis();
					if(--nextlat <= 0) {
						nextlat = 1000;
					}
				}

				sourceContext.collect(new Type(id++, host, time, payload));
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
		see.setNumberOfExecutionRetries(0);

		if(pt.has("timeout")) {
			see.setBufferTimeout(pt.getLong("timeout"));
		}

		if(pt.has("ft")) {
			see.enableCheckpointing(pt.getLong("ft"));
		}

		DataStream<Type> source = see.addSource(new Source(pt) );

		DataStream<Type> repartitioned = source.partitionByHash(0);
		for(int i = 0; i < pt.getInt("repartitions", 1) - 1;i++) {
			repartitioned = repartitioned.map(new MapFunction<Type, Type>() {
				@Override
				public Type map(Type in) throws Exception {
					Type out = in.copy();
					out.f0++;
					return out;
				}
			}).partitionByHash(0);
		}
		repartitioned.flatMap(new FlatMapFunction<Type, Integer>() {
			public int host = -2;
			long received = 0;
			long start = 0;
			long logfreq = pt.getInt("logfreq");
			long lastLog = -1;
			long lastElements = 0;

			@Override
			public void flatMap(Type element, Collector<Integer> collector) throws Exception {
				if(host == -2) {
					host = convertHostnameToInt(InetAddress.getLocalHost().getHostName());
				}
				if (start == 0) {
					start = System.currentTimeMillis();
				}

				received++;
				if (received % logfreq == 0) {
					// throughput over entire time
					long now = System.currentTimeMillis();
					long sinceSec = ((now - start) / 1000);
					if (sinceSec == 0) return;
					LOG.info("Received {} elements since {}. Elements per second {}, GB received {}",
							received,
							sinceSec,
							received / sinceSec,
							(received * (8 + 8 + 4 + pt.getInt("payload"))) / 1024 / 1024 / 1024);

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
					LOG.info("Latency {} ms from machine " + element.f1, lat);
				}
			}
		});
		//System.out.println("plan = "+see.getExecutionPlan());;
		see.execute("Flink Throughput Job with: "+pt.toMap());
	}
}
