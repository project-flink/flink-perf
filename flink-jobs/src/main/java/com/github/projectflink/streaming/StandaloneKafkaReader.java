package com.github.projectflink.streaming;


import com.dataartisans.flink.example.eventpattern.Event;
import com.dataartisans.flink.example.eventpattern.kafka.EventDeSerializer;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

//public class StandaloneKafkaReader {
//	public static Logger LOG = LoggerFactory.getLogger(StandaloneKafkaReader.class);
//
//
//	public static void main(String[] args) {
//		final ParameterTool pt = ParameterTool.fromArgs(args);
//		ConsumerConfig consumerConfig = new ConsumerConfig(pt.getProperties());
//
//		ConsumerConnector consumer = Consumer.createJavaConsumerConnector(consumerConfig);
//		// we request only one stream per consumer instance. Kafka will make sure that each consumer group
//		// will see each message only once.
//		Map<String,Integer> topicCountMap = Collections.singletonMap(pt.getRequired("topic"), 1);
//		Map<String, List<KafkaStream<byte[], byte[]>>> streams = consumer.createMessageStreams(topicCountMap);
//		if(streams.size() != 1) {
//			throw new RuntimeException("Expected only one message stream but got "+streams.size());
//		}
//		List<KafkaStream<byte[], byte[]>> kafkaStreams = streams.get(pt.getRequired("topic"));
//		if(kafkaStreams == null) {
//			throw new RuntimeException("Requested stream not available. Available streams: "+streams.toString());
//		}
//		if(kafkaStreams.size() != 1) {
//			throw new RuntimeException("Requested 1 stream from Kafka, bot got "+kafkaStreams.size()+" streams");
//		}
//		LOG.info("Opening Consumer instance for topic '{}' on group '{}'", pt.getRequired("topic"), consumerConfig.groupId());
//		ConsumerIterator<byte[], byte[]> iteratorToRead = kafkaStreams.get(0).iterator();
//
//		EventDeSerializer serializer = new EventDeSerializer();
//		long received = 0;
//		int logfreq = pt.getInt("logfreq");
//		long lastLog = -1;
//		long lastElements = 0;
//
//		while(iteratorToRead.hasNext()) {
//			MessageAndMetadata<byte[], byte[]> msg = iteratorToRead.next();
//			Event ev = serializer.deserialize(msg.message());
//
//			// print some stats
//			received++;
//			if (received % logfreq == 0) {
//				// throughput over entire time
//				long now = System.currentTimeMillis();
//
//				// throughput for the last "logfreq" elements
//				if(lastLog == -1) {
//					// init (the first)
//					lastLog = now;
//					lastElements = received;
//				} else {
//					long timeDiff = now - lastLog;
//					long elementDiff = received - lastElements;
//					double ex = (1000/(double)timeDiff);
//					LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core. Total read {}", timeDiff, elementDiff, elementDiff*ex, received);
//					// reinit
//					lastLog = now;
//					lastElements = received;
//				}
//			}
//		}
//	}
//}
