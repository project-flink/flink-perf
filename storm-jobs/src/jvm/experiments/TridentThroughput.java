package experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.Grouping;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.Function;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.spout.IBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.util.Map;


public class TridentThroughput {

	public static Logger LOG = LoggerFactory.getLogger(TridentThroughput.class);

	public static class Generator implements IBatchSpout {

		private final int delay;
		private final boolean withFt;
		private final int batchSize;
		private final int latFreq;
		private SpoutOutputCollector spoutOutputCollector;
		private int tid;
		private byte[] payload;
		private long time = 0;

		public Generator(ParameterTool pt) {
			this.payload = new byte[pt.getInt("payload")];
			this.delay = pt.getInt("delay");
			this.withFt = pt.has("ft");
			this.batchSize = pt.getInt("batchSize");
			this.latFreq = pt.getInt("latencyFreq");
		}

		@Override
		public void open(Map conf, TopologyContext context) {
			this.tid = context.getThisTaskId();
		}

		/**
		 * assumes that batchId starts at 0, and is increasing
		 *
		 * @param batchId
		 * @param collector
		 */
		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			long id = (batchId-1) * this.batchSize;
			for(int i = 0; i < this.batchSize; i++) {
				if(id % latFreq == 0) {
					time = System.currentTimeMillis();
				}
				collector.emit(new Values(id, this.tid, this.time, this.payload));
				time = 0;
				id++;
			}
		}

		@Override
		public void ack(long batchId) {
		}

		@Override
		public void close() {

		}

		@Override
		public Map getComponentConfiguration() {
			return null;
		}

		@Override
		public Fields getOutputFields() {
			return new Fields("id", "taskId", "time", "payload");
		}
	}

	public static class IdentityEach implements Function {

		@Override
		public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
			tridentCollector.emit(tridentTuple);
		}
		@Override
		public void prepare(Map map, TridentOperationContext tridentOperationContext) {}
		@Override
		public void cleanup() {}
	}

	public static class Sink implements Function {
		long received = 0;
		long start = 0;
		ParameterTool pt;
		private long logfreq;

		public Sink(ParameterTool pt) {
			this.pt = pt;
			this.logfreq = pt.getInt("logfreq");
		}



		@Override
		public void prepare(Map conf, TridentOperationContext context) {

		}

		@Override
		public void cleanup() {

		}

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {

			if(start == 0) {
				start = System.currentTimeMillis();
			}
			received++;
			if(received % logfreq == 0) {
				long sinceSec = ((System.currentTimeMillis() - start)/1000);
				if(sinceSec == 0) return;
				LOG.info("Received {} elements since {}. Elements per second {}, GB received {}",
						received,
						sinceSec,
						received / sinceSec,
						(received * (8 + 4 + pt.getInt("payload"))) / 1024 / 1024 / 1024);
			}

			if(tuple.getLong(2) != 0) {
				long lat = System.currentTimeMillis() - tuple.getLong(2);
				LOG.info("Latency {} ms", lat);
			}
		}
	}



	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		int par = pt.getInt("para");


		TridentTopology topology = new TridentTopology();
		Stream sourceStream = topology.newStream("source", new Generator(pt)).parallelismHint(pt.getInt("sourceParallelism"));

		Stream repart = sourceStream.partitionBy(new Fields("id"));
		for(int i = 0; i < pt.getInt("repartitions", 1) - 1; i++) {
			repart = repart.each(new IdentityEach(), new Fields("id", "taskId", "time", "payload")).partitionBy(new Fields("id"));
		}
		repart.each(new Fields("id", "taskId", "time", "payload"), new Sink(pt), new Fields("dontcare")).parallelismHint(pt.getInt("sinkParallelism"));

		Config conf = new Config();
		conf.setDebug(false);

		if (!pt.has("local")) {
			conf.setNumWorkers(par);

			StormSubmitter.submitTopologyWithProgressBar("throughput", conf, topology.build());
		}
		else {
			conf.setMaxTaskParallelism(par);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("throughput", conf, topology.build());

			Thread.sleep(30000);

			cluster.shutdown();
		}

	}
}
