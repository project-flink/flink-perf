package com.github.projectflink.streaming;

import com.github.projectflink.generators.Utils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ChainingSpeed {
	private static final Logger LOG = LoggerFactory.getLogger(ChainingSpeed.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);
		see.getConfig().setGlobalJobParameters(pt);
		see.getConfig().enableObjectReuse();

		// see.setParallelism(1);

		DataStreamSource<Integer> src = see.addSource(new RichParallelSourceFunction<Integer>() {

			boolean running = true;
			@Override
			public void run(SourceContext<Integer> ctx) throws Exception {
				int i = 0;
				while (running) {
					ctx.collect(i++);
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		src/*.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer s) throws Exception {
				return s;
			}
		}).*/.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer s) throws Exception {
				return s;
			}
		}).flatMap(new RichFlatMapFunction<Integer, Integer>() {
			long received = 0;
			long logfreq = pt.getInt("logfreq");
			long lastLog = -1;
			long lastElements = 0;
			long matches = 0;
			private final Pattern threeDigitAbbr = Pattern.compile("[A-Z]{3}\\.");

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
			}

			@Override
			public void flatMap(Integer in, Collector<Integer> collector) throws Exception {

				received++;
				if (received % logfreq == 0) {
					// throughput over entire time
					long now = System.currentTimeMillis();

					// throughput for the last "logfreq" elements
					if (lastLog == -1) {
						// init (the first)
						lastLog = now;
						lastElements = received;
					} else {
						long timeDiff = now - lastLog;
						long elementDiff = received - lastElements;
						double ex = (1000 / (double) timeDiff);
						LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core", timeDiff, elementDiff, Double.valueOf(elementDiff * ex).longValue());
						// reinit
						lastLog = now;
						lastElements = received;
					}
				}
			}
		});

		see.execute();
	}
}
