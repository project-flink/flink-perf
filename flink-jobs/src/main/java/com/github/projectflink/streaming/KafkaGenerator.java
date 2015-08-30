package com.github.projectflink.streaming;

import com.dataartisans.flink.example.eventpattern.Event;
import com.dataartisans.flink.example.eventpattern.EventsGenerator;
import com.dataartisans.flink.example.eventpattern.StateMachineMapper;
import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;


public class KafkaGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaGenerator.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);
		see.getConfig().setGlobalJobParameters(pt);
		if(pt.has("p")) {
			see.setParallelism(pt.getInt("p"));
		}

		DataStreamSource<Event> src = see.addSource(new RichParallelSourceFunction<Event>() {

			int min;
			int max;
			EventsGenerator eg;
			boolean running = true;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);

				int range = Integer.MAX_VALUE / getRuntimeContext().getNumberOfParallelSubtasks();
				this.min = range * getRuntimeContext().getIndexOfThisSubtask();
				this.max = min + range;
				eg = new EventsGenerator();
				LOG.info("Creating new EventsGenerator from {} to {}", min, max);
			}


			@Override
			public void run(SourceContext<Event> sourceContext) throws Exception {
				long generated = 0;
				long logfreq = pt.getInt("logfreq");
				long lastLog = -1;
				long lastElements = 0;

				int delay = pt.getInt("delay");
				int sleepFreq = pt.getInt("sleepFreq");

				while (running) {
					if(delay > 0) {
						if(generated % sleepFreq == 0) {
							try { Thread.sleep(delay); } catch (InterruptedException e) { e.printStackTrace();}
						}
					}

					Event gen = eg.next(min, max);
					sourceContext.collect(gen);
					//	LOG.info("Generated event {}", gen);

					generated++;
					if (generated % logfreq == 0) {
						// throughput over entire time
						long now = System.currentTimeMillis();

						// throughput for the last "logfreq" elements
						if (lastLog == -1) {
							// init (the first)
							lastLog = now;
							lastElements = generated;
						} else {
							long timeDiff = now - lastLog;
							long elementDiff = generated - lastElements;
							double ex = (1000 / (double) timeDiff);
							LOG.info("During the last {} ms, we generated {} elements. That's {} elements/second/core", timeDiff, elementDiff, elementDiff * ex);
							// reinit
							lastLog = now;
							lastElements = generated;
						}
					}

				}
			}

			@Override
			public void cancel() {
				running = false;
			}
		});

		// run the state machine here as well to see illegal transitions
		src.partitionByHash(new KeySelector<Event, Integer>() {
			@Override
			public Integer getKey(Event event) throws Exception {
				return event.sourceAddress();
			}
		}).flatMap(new StateMachineMapper(pt));


		// write stuff into Kafka

		String zkServer = pt.get("zookeeper");
		Properties props = pt.getProperties();

		/*if(pt.has("localPartitioner")) {
			SerializableKafkaPartitioner part = new PimpedKafkaSink.LocalKafkaPartitioner(zkServer, pt.getRequired("topic"));
			props.put("partitioner.class", PartitionerWrapper.class.getCanonicalName());
			// java serialization will do the rest.
			props.put(PartitionerWrapper.SERIALIZED_WRAPPER_NAME, part);
		} */

		src.addSink(new KafkaSink<Event>(pt.getRequired("brokerList"), pt.getRequired("topic"), props, new EventDeSerializer()));


		see.execute();
	}
}
