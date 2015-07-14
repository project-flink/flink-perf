package com.github.projectflink.streaming;

import com.dataartisans.flink.example.eventpattern.Event;
import com.dataartisans.flink.example.eventpattern.EventsGenerator;
import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer;
import com.github.projectflink.streaming.utils.PimpedKafkaSink;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(KafkaGenerator.class);

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
		final ParameterTool pt = ParameterTool.fromArgs(args);
		see.getConfig().setGlobalJobParameters(pt);

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

				while(running) {
					Event gen = eg.next(min, max);
					sourceContext.collect(gen);
					LOG.info("Generated event {}", gen);

					generated++;
					if (generated % logfreq == 0) {
						// throughput over entire time
						long now = System.currentTimeMillis();

						// throughput for the last "logfreq" elements
						if(lastLog == -1) {
							// init (the first)
							lastLog = now;
							lastElements = generated;
						} else {
							long timeDiff = now - lastLog;
							long elementDiff = generated - lastElements;
							double ex = (1000/(double)timeDiff);
							LOG.info("During the last {} ms, we generated {} elements. That's {} elements/second/core", timeDiff, elementDiff, elementDiff*ex);
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

		String zkServer = pt.get("zookeeper");
		src.addSink(new KafkaSink<Event>(pt.getRequired("brokerList"), pt.getRequired("topic"), new EventDeSerializer(), new PimpedKafkaSink.LocalKafkaPartitioner(zkServer, pt.getRequired("topic"))));

		see.execute();
	}
}
