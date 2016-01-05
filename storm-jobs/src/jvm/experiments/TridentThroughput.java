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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;


public class TridentThroughput {

	public static Logger LOG = LoggerFactory.getLogger(TridentThroughput.class);

	public static class Generator implements IBatchSpout {

		private final int delay;
		private final boolean withFt;
		private final int batchSize;
		private final int latFreq;
		private SpoutOutputCollector spoutOutputCollector;
		private byte[] payload;
		private long time = 0;
		private int host;
		private int nextlat = 1000;

		public Generator(ParameterTool pt) {
			this.payload = new byte[pt.getInt("payload")];
			this.delay = pt.getInt("delay");
			this.withFt = pt.has("ft");
			this.batchSize = pt.getInt("batchSize");
			this.latFreq = pt.getInt("latencyFreq");
		}

		@Override
		public void open(Map conf, TopologyContext context) {
			try {
				this.host = com.github.projectflink.streaming.Throughput.convertHostnameToInt(InetAddress.getLocalHost().getHostName());
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}

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
				if(id % latFreq == nextlat) {
					time = System.currentTimeMillis();
					if(--nextlat <= 0) {
						nextlat = 1000;
					}
				}
				collector.emit(new Values(id, this.host, this.time, this.payload));
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
			return new Fields("id", "host", "time", "payload");
		}
	}

	public static class IdentityEach implements Function {

		@Override
		public void execute(TridentTuple tridentTuple, TridentCollector tridentCollector) {
			Long id = tridentTuple.getLong(0);
			id++;
			Values v = new Values(id);
			tridentCollector.emit(v);
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
		private int host = -1;
		private long lastLog = -1L;
		private long lastElements;

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
			if(host == -1) {
				try {
					this.host = com.github.projectflink.streaming.Throughput.convertHostnameToInt(InetAddress.getLocalHost().getHostName());
				} catch (UnknownHostException e) {
					e.printStackTrace();
				}
			}
			if(start == 0) {
				start = System.currentTimeMillis();
			}

			received++;
			if(received % logfreq == 0) {
				long now = System.currentTimeMillis();

				/*long sinceSec = ((now - start)/1000);
				if(sinceSec == 0) return;
				LOG.info("Received {} elements since {}. Elements per second {}, GB received {}",
						received,
						sinceSec,
						received/sinceSec ,
						(received * (8 + 8 + 4 + pt.getInt("payload")))/1024/1024/1024 ); */

				// throughput for the last "logfreq" elements
				if(lastLog == -1) {
					// init (the first)
					lastLog = now;
					lastElements = received;
				} else {
					long timeDiff = now - lastLog;
					long elementDiff = received - lastElements;
					double ex = (1000/(double)timeDiff);
					LOG.info("During the last {} ms, we received {} elements. That's {} elements/second/core", timeDiff, elementDiff, elementDiff*ex);
					// reinit
					lastLog = now;
					lastElements = received;
				}
			}

			if(tuple.getLong(2) != 0 && tuple.getInteger(1).equals(host)) {
				long lat = System.currentTimeMillis() - tuple.getLong(2);
				LOG.info("Latency {} ms from machine "+host, lat);
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
			repart = repart.each(new Fields("id"), new IdentityEach(), new Fields("id"+i)).partitionBy(new Fields("id"+i));
		}
		repart.each(new Fields("id", "host", "time", "payload"), new Sink(pt), new Fields("dontcare")).parallelismHint(pt.getInt("sinkParallelism"));

		Config conf = new Config();
		conf.setDebug(false);

		if (!pt.has("local")) {
			conf.setNumWorkers(par);

			StormSubmitter.submitTopologyWithProgressBar("throughput-"+pt.get("name", "no_name"), conf, topology.build());
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
