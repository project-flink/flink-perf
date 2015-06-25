package experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.flink.api.java.utils.ParameterTool;
import org.omg.Dynamic.Parameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Created by robert on 6/25/15.
 */
public class Throughput {

	public static Logger LOG = LoggerFactory.getLogger(Throughput.class);

	public static class Generator extends BaseRichSpout {

		private SpoutOutputCollector spoutOutputCollector;
		private int tid;
		private long id = 0;
		private byte[] payload;
		private ParameterTool pt;
		public Generator(ParameterTool pt) {
			this.pt = pt;
			this.payload = new byte[pt.getInt("payload")];
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
			outputFieldsDeclarer.declare(new Fields("id", "taskId", "payload"));
		}

		@Override
		public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
			this.spoutOutputCollector = spoutOutputCollector;
			this.tid = topologyContext.getThisTaskId();

		}

		@Override
		public void nextTuple() {
			int delay = pt.getInt("delay");
			if(delay > 0) {
				try {
					Thread.sleep(delay);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			spoutOutputCollector.emit(new Values(this.id++, this.tid, this.payload));
		}
	}

	public static class Sink extends BaseBasicBolt {

		long received = 0;
		long start = 0;
		ParameterTool pt;

		public Sink(ParameterTool pt) {
			this.pt = pt;
		}

		@Override
		public void execute(Tuple input, BasicOutputCollector collector) {
			if(start == 0) {
				start = System.currentTimeMillis();
			}
			received++;
			if(received % pt.getInt("logfreq") == 0) {
				long sinceSec = ((System.currentTimeMillis() - start)/1000);
				if(sinceSec == 0) return;
				LOG.info("Received {} elements since {}. Elements per second {}, GB received {}",
						received,
						sinceSec,
						received/sinceSec ,
						(received * (8 + 4 + pt.getInt("payload")))/1024/1024/1024 );
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}
	}

	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		int par = pt.getInt("para");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("source", new Generator(pt), pt.getInt("sourceParallelism"));
		builder.setBolt("sink", new Sink(pt), pt.getInt("sinkParallelism")).fieldsGrouping("source", new Fields("id"));


		Config conf = new Config();
		conf.setDebug(true);

		if (!pt.has("local")) {
			conf.setNumWorkers(par);

			StormSubmitter.submitTopologyWithProgressBar("throughput", conf, builder.createTopology());
		}
		else {
			conf.setMaxTaskParallelism(par);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("throughput", conf, builder.createTopology());

			Thread.sleep(10000);

			cluster.shutdown();
		}

	}
}
