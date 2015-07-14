package com.github.projectflink.streaming.utils;

import com.google.common.base.Preconditions;
import com.google.common.collect.HashMultimap;
import kafka.cluster.Broker;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.DefaultEncoder;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.api.config.PartitionerWrapper;
import org.apache.flink.streaming.connectors.kafka.api.persistent.PersistentKafkaSource;
import org.apache.flink.streaming.connectors.kafka.partitioner.SerializableKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.apache.flink.util.NetUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.mutable.Buffer;
import scala.collection.mutable.Seq;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PimpedKafkaSink<IN> /*extends RichSinkFunction<IN> */ {

	public static class LocalKafkaPartitioner implements SerializableKafkaPartitioner {
		private static final Logger LOG = LoggerFactory.getLogger(LocalKafkaPartitioner.class);

		private final HashMultimap<String, Integer> mapping;
		private String host;
		private ArrayList<Integer> partitions;
		private int index = 0;

		public LocalKafkaPartitioner(String zkServer, String topicName) {
			// get mapping hostname(string)->partitionId
			ZkClient zkClient = new ZkClient(zkServer, 1000, 1000, new PersistentKafkaSource.KafkaZKStringSerializer());

			scala.collection.Seq<Broker> zkBrokersScala = ZkUtils.getAllBrokersInCluster(zkClient);
			Collection<Broker> zkBrokersColl = JavaConversions.asJavaCollection(zkBrokersScala);


			List<String> topics = new ArrayList<String>();
			topics.add(topicName);
			Buffer<String> scBuf = JavaConversions.asScalaBuffer(topics);
			scala.collection.mutable.Map<String, scala.collection.Seq<Object>> partForTopics = ZkUtils.getPartitionsForTopics(zkClient, scBuf);
			Option<scala.collection.Seq<Object>> topicOpt = partForTopics.get(topicName);
			scala.collection.Seq<Object> topicSeq = topicOpt.get();

			Collection<Object> partitionIds = JavaConversions.asJavaCollection(topicSeq);

			if(partitionIds.size() == 0) {
				throw new RuntimeException("The topic "+topicName+" does not have any partitions");
			}
			// Map<String, Integer> mapping = new HashMap<String, Integer>(partitionIds.size());
			this.mapping = HashMultimap.create();

			for(Object partId : partitionIds) {
				Option<Object> leaderIdOption = ZkUtils.getLeaderForPartition(zkClient, topicName, (Integer) partId);
				Object leaderId = leaderIdOption.get();
				for(Broker b : zkBrokersColl) {
					if( (Integer)leaderId == b.id()) {
						mapping.put(b.host(), (Integer)partId);
					}
				}
			}
			LOG.info("Created mapping " + mapping);
		}

 		@Override
		public int partition(Object key, int numPartitions) {
			if(host == null) {
				try {
					host = InetAddress.getLocalHost().getHostName();
				} catch (UnknownHostException e) {
					throw new RuntimeException("Can not get host. Locality aware partitioning not possible", e);
				}
				for(Map.Entry<String, Integer> entry : mapping.entries()) {
					if(entry.getKey().contains(host) || host.contains(entry.getKey())) {
						if(partitions != null) {
							throw new RuntimeException("There was already a match for host "+host+" in "+mapping);
						}
						partitions = new ArrayList<Integer>(mapping.get(entry.getKey()));
					}
				}
				//partitions = new ArrayList<Integer>(mapping.get(host));
				LOG.info("Host {} is going to send data to partitions: {}", host, partitions);
			}

			int part = partitions.get(index++);
			if(index == partitions.size()) {
				index = 0;
			}
			return part;
		}
	}

//	private static final long serialVersionUID = 1L;
//
//	private static final Logger LOG = LoggerFactory.getLogger(PimpedKafkaSink.class);
//
//	private Producer<IN, byte[]> producer;
//	private Properties userDefinedProperties;
//	private String topicId;
//	private String brokerList;
//	private SerializationSchema<IN, byte[]> schema;
//	private SerializableKafkaPartitioner partitioner;
//	private Class<? extends SerializableKafkaPartitioner> partitionerClass = null;
//
//	/**
//	 * Creates a KafkaSink for a given topic. The sink produces its input to
//	 * the topic.
//	 *
//	 * @param brokerList
//	 *			Addresses of the brokers
//	 * @param topicId
//	 * 		ID of the Kafka topic.
//	 * @param serializationSchema
//	 * 		User defined serialization schema.
//	 */
//	public PimpedKafkaSink(String brokerList, String topicId,
//					 SerializationSchema<IN, byte[]> serializationSchema) {
//		this(brokerList, topicId, new Properties(), serializationSchema);
//	}
//
//	/**
//	 * Creates a KafkaSink for a given topic with custom Producer configuration.
//	 * If you use this constructor, the broker should be set with the "metadata.broker.list"
//	 * configuration.
//	 *
//	 * @param brokerList
//	 * 		Addresses of the brokers
//	 * @param topicId
//	 * 		ID of the Kafka topic.
//	 * @param producerConfig
//	 * 		Configurations of the Kafka producer
//	 * @param serializationSchema
//	 * 		User defined serialization schema.
//	 */
//	public PimpedKafkaSink(String brokerList, String topicId, Properties producerConfig,
//					 SerializationSchema<IN, byte[]> serializationSchema) {
//		String[] elements = brokerList.split(",");
//		for(String broker: elements) {
//			NetUtils.ensureCorrectHostnamePort(broker);
//		}
//		Preconditions.checkNotNull(topicId, "TopicID not set");
//
//		this.brokerList = brokerList;
//		this.topicId = topicId;
//		this.schema = serializationSchema;
//		this.partitionerClass = null;
//		this.userDefinedProperties = producerConfig;
//	}
//
//	/**
//	 * Creates a KafkaSink for a given topic. The sink produces its input to
//	 * the topic.
//	 *
//	 * @param brokerList
//	 * @param topicId
//	 * 		ID of the Kafka topic.
//	 * @param serializationSchema
//	 * 		User defined serialization schema.
//	 * @param partitioner
//	 * 		User defined partitioner.
//	 */
//	public PimpedKafkaSink(String brokerList, String topicId,
//					 SerializationSchema<IN, byte[]> serializationSchema, SerializableKafkaPartitioner partitioner) {
//		this(brokerList, topicId, serializationSchema);
//		ClosureCleaner.ensureSerializable(partitioner);
//		this.partitioner = partitioner;
//	}
//
//	public PimpedKafkaSink(String brokerList,
//					 String topicId,
//					 SerializationSchema<IN, byte[]> serializationSchema,
//					 Class<? extends SerializableKafkaPartitioner> partitioner) {
//		this(brokerList, topicId, serializationSchema);
//		this.partitionerClass = partitioner;
//	}
//
//	/**
//	 * Initializes the connection to Kafka.
//	 */
//	@Override
//	public void open(Configuration configuration) throws UnknownHostException {
//
//		Properties properties = new Properties();
//		List<String> brokers = new ArrayList<String>();
//		String[] elements = brokerList.split(",");
//		String host = InetAddress.getLocalHost().getHostName();
//		for(String broker: elements) {
//			if(broker.contains(host)) {
//				brokers.add(broker);
//			}
//		}
//
//		properties.put("metadata.broker.list", StringUtils.join(brokers, ","));
//
//		properties.put("request.required.acks", "-1");
//		properties.put("message.send.max.retries", "10");
//
//		properties.put("serializer.class", DefaultEncoder.class.getCanonicalName());
//
//		// this will not be used as the key will not be serialized
//		properties.put("key.serializer.class", DefaultEncoder.class.getCanonicalName());
//
//		for (Map.Entry<Object, Object> propertiesEntry : userDefinedProperties.entrySet()) {
//			properties.put(propertiesEntry.getKey(), propertiesEntry.getValue());
//		}
//
//		if (partitioner != null) {
//			properties.put("partitioner.class", PartitionerWrapper.class.getCanonicalName());
//			// java serialization will do the rest.
//			properties.put(PartitionerWrapper.SERIALIZED_WRAPPER_NAME, partitioner);
//		}
//		if (partitionerClass != null) {
//			properties.put("partitioner.class", partitionerClass);
//		}
//
//		ProducerConfig config = new ProducerConfig(properties);
//
//		try {
//			producer = new Producer<IN, byte[]>(config);
//		} catch (NullPointerException e) {
//			throw new RuntimeException("Cannot connect to Kafka broker " + brokerList, e);
//		}
//	}
//
//	/**
//	 * Called when new data arrives to the sink, and forwards it to Kafka.
//	 *
//	 * @param next
//	 * 		The incoming data
//	 */
//	@Override
//	public void invoke(IN next) {
//		byte[] serialized = schema.serialize(next);
//
//		// Sending message without serializable key.
//		producer.send(new KeyedMessage<IN, byte[]>(topicId, null, next, serialized));
//	}
//
//	@Override
//	public void close() {
//		if (producer != null) {
//			producer.close();
//		}
//	}
}
