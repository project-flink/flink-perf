package com.github.projectflink.streaming;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.checkpoint.Checkpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Throughput {

	private static final Logger LOG = LoggerFactory.getLogger(Throughput.class);

	public static class Source extends RichParallelSourceFunction<Tuple3<Long, Integer, byte[]>> implements Checkpointed<Long> {

		final ParameterTool pt;
		byte[] payload;
		long id = 0;
		boolean running = true;

		public Source(ParameterTool pt) {
			this.pt = pt;
			payload = new byte[pt.getInt("payload")];
		}

		@Override
		public void run(SourceContext<Tuple3<Long, Integer, byte[]>> sourceContext) throws Exception {
			int delay = pt.getInt("delay");
			while(running) {
				if (delay > 0) {
					try {
						Thread.sleep(delay);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
				sourceContext.collect(new Tuple3<Long, Integer, byte[]>(id++, getRuntimeContext().getIndexOfThisSubtask(), payload));
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

		if(pt.has("ft")) {
			see.enableCheckpointing(pt.getLong("ft"));
		}

		DataStream<Tuple3<Long, Integer, byte[]>> source = see.addSource(new Source(pt) );

		source.partitionByHash(0).flatMap(new FlatMapFunction<Tuple3<Long,Integer,byte[]>, Integer>() {
			long received = 0;
			long start = 0;
			long logfreq = pt.getInt("logfreq");
			@Override
			public void flatMap(Tuple3<Long, Integer, byte[]> element, Collector<Integer> collector) throws Exception {
				if(start == 0) {
					start = System.currentTimeMillis();
				}
				received++;
				if(received % logfreq == 0) {
					long sinceSec = ((System.currentTimeMillis() - start)/1000);
					if(sinceSec == 0) return;
					LOG.info("Received {} elements since {}. Elements per second {}, GB received {}",
							received,
							sinceSec,
							received/sinceSec ,
							(received * (8 + 4 + pt.getInt("payload")))/1024/1024/1024 );
				}
			}
		});

		see.execute();
	}
}
