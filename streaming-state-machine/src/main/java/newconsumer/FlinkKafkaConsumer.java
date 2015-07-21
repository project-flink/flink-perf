package newconsumer;

import org.apache.commons.collections.map.LinkedMap;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
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
	private transient boolean running = true;
	private final LinkedMap pendingCheckpoints = new LinkedMap();
	private long[] lastOffsets;

	public FlinkKafkaConsumer(String topic, DeserializationSchema<T> valueDeserializer, Properties props) {
		this.topic = topic;
		this.props = props;
		this.valueDeserializer = valueDeserializer;

		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<byte[], byte[]>(props, null, new ByteArrayDeserializer(), new ByteArrayDeserializer());
		List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);
		partitions = new ArrayList<TopicPartition>(partitionInfos.size());
		for(int i = 0; i < partitions.size(); i++) {
			partitions.add(convert(partitionInfos.get(i)));
		}
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
		consumer.subscribe(partitionsToSub.toArray(new TopicPartition[partitionsToSub.size()]));
		lastOffsets = new long[partitions.size()];
		Arrays.fill(lastOffsets, -1);
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
			ConsumerRecords<byte[], byte[]> consumed = consumer.poll(pollTimeout);
			if(!consumed.isEmpty()) {
				for(ConsumerRecord<byte[], byte[]> record : consumed) {
					T value = valueDeserializer.deserialize(record.value());

				}
			}
		}

	}

	@Override
	public void cancel() {
		running = false;
		consumer.close();
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

		Map<TopicPartition, Long> offsetsToCommit = new HashMap<TopicPartition, Long>();
		for(int i = 0; i < checkpointOffsets.length; i++) {
			if(checkpointOffsets[i] != -1) {
				offsetsToCommit.put(new TopicPartition(topic, i), checkpointOffsets[i]);
			}
		}

		consumer.commit(offsetsToCommit, CommitType.SYNC);
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

	}



}
