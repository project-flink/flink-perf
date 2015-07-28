package com.github.projectflink.streaming;

import com.github.projectflink.generators.Utils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ForwardThroughput {
	private static final Logger LOG = LoggerFactory.getLogger(ForwardThroughput.class);


	public static class Type extends Tuple2<String, Long> {
		public Type(String value0, Long value2) {
			super(value0, value2);
		}

		public Type() {
		}
	}
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);
		see.getConfig().setGlobalJobParameters(pt);
		// see.getConfig().enableObjectReuse();

		// see.setParallelism(1);

		if(pt.has("timeout")) {
			see.setBufferTimeout(pt.getLong("timeout"));
		}

		if(pt.has("ft")) {
			see.enableCheckpointing(pt.getLong("ft"));
		}

		DataStreamSource<Type> src = see.addSource(new RichParallelSourceFunction<Type>() {

			String[] texts;
			boolean running = true;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				Random rnd = new Random(1337);
				texts = new String[pt.getInt("words")];
				for (int i = 0; i < pt.getInt("words"); i++) {
					String str = "";
					int sentenceLength = rnd.nextInt(25); // up to 16 words per sentence
					for (int s = 0; s < sentenceLength; s++) {
						str += Utils.getFastZipfRandomWord();
						str += " ";
					}
					texts[i] = str;
				}
			}

			@Override
			public void run(SourceContext<Type> ctx) throws Exception {
				int i = 0;
				long time = 0L;
				long id = 0;
				int latFreq = pt.getInt("latencyFreq");
				while (running) {
					if(id++ % latFreq == 0) {
						time = System.currentTimeMillis();
					}

					ctx.collect(new Type(texts[i++], time));
					if (i == texts.length) {
						i = 0;
					}
					time = 0L;
				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		src.flatMap(new RichFlatMapFunction<Type, Integer>() {
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
			public void flatMap(Type element, Collector<Integer> collector) throws Exception {
				Matcher m = threeDigitAbbr.matcher(element.f0);
				if (m.matches()) {
					matches++;
				}

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
				if(element.f1 != 0L) {
					long lat = System.currentTimeMillis() - element.f1;
					LOG.info("Latency {} ms from same machine", lat);
				}
			}
		});

		see.execute("Forward Throughout "+pt.toMap());
	}
}
