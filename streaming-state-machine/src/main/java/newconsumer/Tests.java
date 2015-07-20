package newconsumer;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.accumulators.Histogram;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.cache.DistributedCache;
import org.apache.flink.api.common.functions.BroadcastVariableInitializer;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.OperatorState;
import org.apache.flink.api.common.state.StateCheckpointer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class Tests {

	@Test
	public void testAssignment() {
		FlinkKafkaConsumer c = mock(FlinkKafkaConsumer.class);
		List<PartitionInfo> parts = new ArrayList<PartitionInfo>();
		for(int i = 0; i < 599; i++) {
			parts.add(new PartitionInfo("", i, null, null, null));
		}

		FakeRuntimeCtx ctx = new FakeRuntimeCtx();

		when(c.getPartitions()).thenReturn(parts);
		when(c.getRuntimeContext()).thenReturn(ctx);

		List<TopicPartition> assignment = c.assignPartitions();

		Assert.assertEquals(1, assignment.size());

	}

	public static class FakeRuntimeCtx implements RuntimeContext {

		@Override
		public String getTaskName() {
			return null;
		}

		@Override
		public int getNumberOfParallelSubtasks() {
			return 600;
		}

		@Override
		public int getIndexOfThisSubtask() {
			return 2;
		}

		@Override
		public ExecutionConfig getExecutionConfig() {
			return null;
		}

		@Override
		public ClassLoader getUserCodeClassLoader() {
			return null;
		}

		@Override
		public <V, A extends Serializable> void addAccumulator(String s, Accumulator<V, A> accumulator) {

		}

		@Override
		public <V, A extends Serializable> Accumulator<V, A> getAccumulator(String s) {
			return null;
		}

		@Override
		public Map<String, Accumulator<?, ?>> getAllAccumulators() {
			return null;
		}

		@Override
		public IntCounter getIntCounter(String s) {
			return null;
		}

		@Override
		public LongCounter getLongCounter(String s) {
			return null;
		}

		@Override
		public DoubleCounter getDoubleCounter(String s) {
			return null;
		}

		@Override
		public Histogram getHistogram(String s) {
			return null;
		}

		@Override
		public <RT> List<RT> getBroadcastVariable(String s) {
			return null;
		}

		@Override
		public <T, C> C getBroadcastVariableWithInitializer(String s, BroadcastVariableInitializer<T, C> broadcastVariableInitializer) {
			return null;
		}

		@Override
		public DistributedCache getDistributedCache() {
			return null;
		}

		@Override
		public <S, C extends Serializable> OperatorState<S> getOperatorState(String s, S s1, boolean b, StateCheckpointer<S, C> stateCheckpointer) throws IOException {
			return null;
		}

		@Override
		public <S extends Serializable> OperatorState<S> getOperatorState(String s, S s1, boolean b) throws IOException {
			return null;
		}
	}
}

