package com.github.projectflink.streaming;

import com.dataartisans.flink.example.eventpattern.Event;
import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Minimalistic Flink Kafka reader to measure read performance (similar to the Standalone kafka reader
 */
public class FlinkKafkaReader {

	public static Logger LOG = LoggerFactory.getLogger(FlinkKafkaReader.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);

		DataStreamSource<Event> src = see.addSource(new FlinkKafkaConsumer<Event>(pt.getRequired("topic"), new EventDeSerializer(), pt.getProperties()));

		src.flatMap(new FlatMapFunction<Event, Integer>() {
			long received = 0;
			int logfreq = pt.getInt("logfreq");
			long lastLog = -1;
			long lastElements = 0;

			@Override
			public void flatMap(Event event, Collector<Integer> collector) throws Exception {


				// print some stats
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
						LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. Total read {}", timeDiff, elementDiff, elementDiff*ex, received);
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
