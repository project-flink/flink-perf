package newconsumer;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.checkpoint.CheckpointNotifier;
import org.apache.flink.streaming.api.checkpoint.CheckpointedAsynchronously;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaConsumer<T> extends RichParallelSourceFunction<T> implements CheckpointNotifier, CheckpointedAsynchronously<long[]> {

	private final String topic;
	private final Deserializer<T> valueDeserializer;
	private final Properties props;
	private final List<PartitionInfo> partitions;

	private transient KafkaConsumer<T, byte[]> consumer;

	public FlinkKafkaConsumer(String topic, Deserializer<T> valueDeserializer, Properties props) {
		this.topic = topic;
		this.valueDeserializer = valueDeserializer; // todo check for serializer implementing serializer interface as well
		this.props = props;

		KafkaConsumer<T, byte[]> consumer = new KafkaConsumer<T, byte[]>(props);
		partitions = consumer.partitionsFor(topic);
		consumer.close();
	}

	// ----------------------------- Source ------------------------------

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);


		List<TopicPartition> partitionsToSub = assignPartitions();

		// create consumer
		consumer = new KafkaConsumer<T, byte[]>(props);
		consumer.subscribe((TopicPartition[]) partitionsToSub.toArray());

	}
	protected List<PartitionInfo> getPartitions() {
		return partitions;
	}

	public List<TopicPartition> assignPartitions() {
		List<PartitionInfo> parts = getPartitions();
		int topicsPerInstance = (int)Math.floor(getRuntimeContext().getNumberOfParallelSubtasks() / parts.size());
		List<TopicPartition> partitionsToSub = new ArrayList<TopicPartition>(topicsPerInstance);


		return partitionsToSub;
	}

	@Override
	public void run(SourceContext<T> sourceContext) throws Exception {

	}

	@Override
	public void cancel() {

	}


	// ----------------------------- State ------------------------------
	@Override
	public void notifyCheckpointComplete(long l) throws Exception {

	}

	@Override
	public long[] snapshotState(long l, long l1) throws Exception {
		return new long[0];
	}

	@Override
	public void restoreState(long[] longs) {

	}

}
