package newconsumer;

import kafka.common.TopicAndPartition;
import kafka.utils.ZKGroupTopicDirs;
import kafka.utils.ZkUtils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.kafka.copied.clients.consumer.CommitType;
import org.apache.kafka.copied.clients.consumer.Consumer;
import org.apache.kafka.copied.clients.consumer.ConsumerConfig;
import org.apache.kafka.copied.clients.consumer.ConsumerRebalanceCallback;
import org.apache.kafka.copied.clients.consumer.ConsumerRecord;
import org.apache.kafka.copied.clients.consumer.ConsumerRecords;
import org.apache.kafka.copied.clients.consumer.KafkaConsumer;
import org.apache.kafka.copied.common.PartitionInfo;
import org.apache.kafka.copied.common.TopicPartition;
import org.apache.kafka.copied.common.serialization.ByteArrayDeserializer;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Option;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class FlinkKafkaConsumer<T> extends RichParallelSourceFunction<T>
		implements CheckpointNotifier, CheckpointedAsynchronously<long[]> {

	public static Logger LOG = LoggerFactory.getLogger(FlinkKafkaConsumer.class);

	public final static String POLL_TIMEOUT = "flink.kafka.consumer.poll.timeout";
	public final static long DEFAULT_POLL_TIMEOUT = 50;

	private final String topic;
	private final Properties props;
	private final List<TopicPartition> partitions;
	private final DeserializationSchema<T> valueDeserializer;

	private transient KafkaConsumer<byte[], byte[]> consumer;
	private boolean running = true;
	private final LinkedMap pendingCheckpoints = new LinkedMap();
	private long[] lastOffsets;
	private long[] commitedOffsets;
	private ZkClient zkClient;
	private long[] restoreToOffset;

	public FlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this.topic = topic;
		this.props = props; // TODO check for zookeeper properties
		this.valueDeserializer = valueDeserializer;

		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props, null, new ByteArrayDeserializer(), new ByteArrayDeserializer());
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
		if(partitionInfos == null) {
			throw new RuntimeException("The topic "+topic+" does not seem to exist");
		}
		partitions = new ArrayList<TopicPartition>(partitionInfos.size());
		for(int i = 0; i < partitionInfos.size(); i++) {
			partitions.add(convert(partitionInfos.get(i)));
		}
		LOG.info("Topic {} has {} partitions", topic, partitions.size());
		consumer.close();
	}

	// ----------------------------- Source ------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);

		List<TopicPartition> partitionsToSub = assignPartitions();

		// make sure that we take care of the committing
		props.setProperty("enable.auto.commit", "false");

		// create consumer
		consumer = new KafkaConsumer<byte[], byte[]>(props, null, new ByteArrayDeserializer(), new ByteArrayDeserializer());

		LOG.info("This instance is going to subscribe to partitions {}", partitionsToSub);
		// subscribe
		consumer.subscribe(partitionsToSub.toArray(new TopicPartition[partitionsToSub.size()]));

		// set up operator state
		lastOffsets = new long[partitions.size()];
		Arrays.fill(lastOffsets, -1);

		// prepare Zookeeper
		zkClient = new ZkClient(props.getProperty("zookeeper.connect"),
				Integer.valueOf(props.getProperty("zookeeper.session.timeout.ms", "6000")),
				Integer.valueOf(props.getProperty("zookeeper.connection.timeout.ms", "6000")),
				new KafkaZKStringSerializer());
		commitedOffsets = new long[partitions.size()];


		// seek to last known pos, from restore request
		if(restoreToOffset != null) {
			LOG.info("Found offsets to restore to.");
			for(int i = 0; i < restoreToOffset.length; i++) {
				// if this fails because we are not subscribed to the topic, the partition assignment is not deterministic!
				consumer.seek(new TopicPartition(topic, i), restoreToOffset[i]);
			}
		} else {
			// no restore request. See what we have in ZK for this consumer group
			for(TopicPartition tp: partitionsToSub) {
				long offset = getOffset(zkClient, props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), topic, tp.partition());
				if(offset != -1) {
					LOG.info("Offset for partition {} was set to {} in ZK. Seeking consumer to that position", tp.partition(), offset);
					consumer.seek(tp, offset);
				}
			}
		}

	}

	protected List<TopicPartition> getPartitions() {
		return partitions;
	}

	public List<TopicPartition> assignPartitions() {
		List<TopicPartition> parts = getPartitions();
		List<TopicPartition> partitionsToSub = new ArrayList<TopicPartition>();

		int machine = 0;
		for(int i = 0; i < parts.size(); i++) {
			if(machine == getRuntimeContext().getIndexOfThisSubtask()) {
				partitionsToSub.add(parts.get(i));
			}
			machine++;

			if(machine == getRuntimeContext().getNumberOfParallelSubtasks()) {
				machine = 0;
			}
		}

		return partitionsToSub;
	}

	private static TopicPartition convert(PartitionInfo info) {
		return new TopicPartition(info.topic(), info.partition());
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {
		long pollTimeout = DEFAULT_POLL_TIMEOUT;
		if(props.contains(POLL_TIMEOUT)) {
			pollTimeout = Long.valueOf(props.getProperty(POLL_TIMEOUT));
		}
		while(running) {
			synchronized (consumer) {
				ConsumerRecords<byte[], byte[]> consumed = consumer.poll(pollTimeout);
				if(!consumed.isEmpty()) {
					synchronized (sourceContext.getCheckpointLock()) {
						for(ConsumerRecord<byte[], byte[]> record : consumed) {
							T value = valueDeserializer.deserialize(record.value());
							sourceContext.collect(value);
							lastOffsets[record.partition()] = record.offset();
							LOG.info("Consumed " + value);
						}
					}
				}
			}
		}

	}

	@Override
	public void cancel() {
		running = false;
		synchronized (consumer) { // TODO: cancel leads to a deadlock. Seems that poll is not releasing its lock on consumer.
			consumer.close();
		}
	}


	// ----------------------------- State ------------------------------
	@Override
	public void notifyCheckpointComplete(long checkpointId) throws Exception {
		if(consumer == null) {
			LOG.info("notifyCheckpointComplete() called on uninitialized source");
			return;
		}

		LOG.info("Commit checkpoint {}", checkpointId);

		long[] checkpointOffsets;

		// the map may be asynchronously updates when snapshotting state, so we synchronize
		synchronized (pendingCheckpoints) {
			final int posInMap = pendingCheckpoints.indexOf(checkpointId);
			if (posInMap == -1) {
				LOG.warn("Unable to find pending checkpoint for id {}", checkpointId);
				return;
			}

			checkpointOffsets = (long[]) pendingCheckpoints.remove(posInMap);
			// remove older checkpoints in map:
			if (!pendingCheckpoints.isEmpty()) {
				for(int i = 0; i < posInMap; i++) {
					pendingCheckpoints.remove(0);
				}
			}
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Committing offsets {} to Kafka Consumer", Arrays.toString(checkpointOffsets));
		}

		setOffsetsInZooKeeper(checkpointOffsets);

		/*
		TODO: enable for users using kafka brokers with a central coordinator.

		Map<TopicPartition, Long> offsetsToCommit = new HashMap<TopicPartition, Long>();
		for(int i = 0; i < checkpointOffsets.length; i++) {
			if(checkpointOffsets[i] != -1) {
				offsetsToCommit.put(new TopicPartition(topic, i), checkpointOffsets[i]);
			}
		}

		synchronized (consumer) {
			consumer.commit(offsetsToCommit, CommitType.SYNC);
		} */
	}

	@Override
	public long[] snapshotState(long checkpointId, long checkpointTimestamp) throws Exception {
		if (lastOffsets == null) {
			LOG.warn("State snapshot requested on not yet opened source. Returning null");
			return null;
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Snapshotting state. Offsets: {}, checkpoint id {}, timestamp {}",
					Arrays.toString(lastOffsets), checkpointId, checkpointTimestamp);
		}

		long[] currentOffsets = Arrays.copyOf(lastOffsets, lastOffsets.length);

		// the map may be asynchronously updates when committing to Kafka, so we synchronize
		synchronized (pendingCheckpoints) {
			pendingCheckpoints.put(checkpointId, currentOffsets);
		}

		return currentOffsets;
	}

	@Override
	public void restoreState(long[] restoredOffsets) {
		restoreToOffset = restoredOffsets;
	}

	// ---------- Zookeeper communication ----------------

	private void setOffsetsInZooKeeper(long[] offsets) {
		for (int partition = 0; partition < offsets.length; partition++) {
			long offset = offsets[partition];
			if(offset != -1) {
				setOffset(partition, offset);
			}
		}
	}

	protected void setOffset(int partition, long offset) {
		// synchronize because notifyCheckpointComplete is called using asynchronous worker threads (= multiple checkpoints might be confirmed concurrently)
		synchronized (commitedOffsets) {
			if(commitedOffsets[partition] < offset) {
				LOG.info("Committed offsets {}, partition={}, offset={}", Arrays.toString(commitedOffsets), partition, offset);
				setOffset(zkClient, props.getProperty(ConsumerConfig.GROUP_ID_CONFIG), topic, partition, offset);
				commitedOffsets[partition] = offset;
			} else {
				LOG.debug("Ignoring offset {} for partition {} because it is already committed", offset, partition);
			}
		}
	}



	// the following two methods are static to allow access from the outside as well (Testcases)

	/**
	 * This method's code is based on ZookeeperConsumerConnector.commitOffsetToZooKeeper()
	 */
	public static void setOffset(ZkClient zkClient, String groupId, String topic, int partition, long offset) {
		LOG.info("Setting offset for partition {} of topic {} in group {} to offset {}", partition, topic, groupId, offset);
		TopicAndPartition tap = new TopicAndPartition(topic, partition);
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, tap.topic());
		ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir() + "/" + tap.partition(), Long.toString(offset));
	}

	public static long getOffset(ZkClient zkClient, String groupId, String topic, int partition) {
		TopicAndPartition tap = new TopicAndPartition(topic, partition);
		ZKGroupTopicDirs topicDirs = new ZKGroupTopicDirs(groupId, tap.topic());
		scala.Tuple2<Option<String>, Stat> data = ZkUtils.readDataMaybeNull(zkClient, topicDirs.consumerOffsetDir() + "/" + tap.partition());
		if(data._1().isEmpty()) {
			return -1;
		} else {
			return Long.valueOf(data._1().get());
		}
	}

	// ---------------------- Zookeeper Serializer copied from Kafka (because it has private access there)  -----------------
	public static class KafkaZKStringSerializer implements ZkSerializer {

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			try {
				return ((String) data).getBytes("UTF-8");
			} catch (UnsupportedEncodingException e) {
				throw new RuntimeException(e);
			}
		}

		@Override
		public Object deserialize(byte[] bytes) throws ZkMarshallingError {
			if (bytes == null) {
				return null;
			} else {
				try {
					return new String(bytes, "UTF-8");
				} catch (UnsupportedEncodingException e) {
					throw new RuntimeException(e);
				}
			}
		}
	}


}
