package experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
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
import com.github.projectflink.generators.Utils;
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

import java.net.UnknownHostException;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Args for local:
 *
 * --para 4 --sourceParallelism 2 --sinkParallelism 2 --logfreq 100000 --words 2000 --delay 0 --sleepFreq 0 --latencyFreq 100000 --local
 */
public class TridentForwardThroughput {

	public static Logger LOG = LoggerFactory.getLogger(TridentForwardThroughput.class);

	public static Fields FIELDS = new Fields("string", "time");

	public static class Generator implements IBatchSpout {

		private final int delay;
		private final int latFreq;
		private final ParameterTool pt;
		private long id = 0;
		private long time = 0;
		private int sleepFreq;
		private String[] texts;
		private int i = 0;
		private int batchSize;

		public Generator(ParameterTool pt) {
			this.delay = pt.getInt("delay");
			this.latFreq = pt.getInt("latencyFreq");
			this.sleepFreq = pt.getInt("sleepFreq");
			this.pt = pt;
			this.batchSize = pt.getInt("batchSize", 1000);
		}


		@Override
		public void open(Map conf, TopologyContext context) {
			Random rnd = new Random(1337);
			texts = new String[pt.getInt("words")];
			int totLength = 0;
			for (int i = 0; i < pt.getInt("words"); i++) {
				String str = "";
				int sentenceLength = rnd.nextInt(25); // up to 16 words per sentence
				for (int s = 0; s < sentenceLength; s++) {
					str += Utils.getFastZipfRandomWord();
					str += " ";
				}
				totLength += str.length();
				texts[i] = str;
			}
			LOG.info("Average string length "+(totLength/(double)pt.getInt("words")));
		}

		@Override
		public void emitBatch(long batchId, TridentCollector collector) {
			int texts_i = 0;
			for(int i = 0; i < batchSize; i++) {
				if(delay > 0) {
					if(id % sleepFreq == 0) {
						try { Thread.sleep(delay); } catch (InterruptedException e) { e.printStackTrace();}
					}
				}
				// send time at beginning and end of batch
				if(i == 0 || i + 1 == batchSize) {
					time = System.currentTimeMillis();
				}

				collector.emit(new Values(texts[texts_i], this.time));

				if(++texts_i == texts.length) {
					texts_i = 0;
				}

				time = 0;
				this.id++;
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
			return FIELDS;
		}
	}


	public static class Sink implements Function {

		private final boolean withFT;
		long received = 0;
		long start = 0;
		ParameterTool pt;
		private long logfreq;
		private long lastLog = -1;
		private long lastElements;
		private long matches = 0;

		private final Pattern threeDigitAbbr = Pattern.compile("[A-Z]{3}\\.");

		public Sink(ParameterTool pt) throws UnknownHostException {
			this.pt = pt;
			this.withFT = pt.has("ft");
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
			Matcher m = threeDigitAbbr.matcher(tuple.getString(0));
			if (m.matches()) {
				matches++;
			}

			if(start == 0) {
				start = System.currentTimeMillis();
			}
			received++;
			if(received % logfreq == 0) {
				long now = System.currentTimeMillis();


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

			if(tuple.getLong(1) != 0) {
				long lat = System.currentTimeMillis() - tuple.getLong(1);
				LOG.info("Latency {} ms from same machine", lat);
			}
		}
	}



	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		int par = pt.getInt("para");

		TridentTopology topology = new TridentTopology();
		Stream sourceStream = topology.newStream("source", new Generator(pt)).parallelismHint(pt.getInt("sourceParallelism"));
		sourceStream.each(FIELDS, new Sink(pt), new Fields("dontcare"));

		Config conf = new Config();
		conf.setDebug(false);

	//	conf.setMaxSpoutPending(pt.getInt("maxPending", 1000));

		//System.exit(1);

		if (!pt.has("local")) {
			conf.setNumWorkers(par);

			StormSubmitter.submitTopologyWithProgressBar("forward-throughput-"+pt.get("name", "no_name"), conf, topology.build());
		}
		else {
			conf.setMaxTaskParallelism(par);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("forward-throughput", conf, topology.build());

			Thread.sleep(300000);

			cluster.shutdown();
		}

	}
}
