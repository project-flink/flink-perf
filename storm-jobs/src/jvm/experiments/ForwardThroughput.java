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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * Args for local:
 *
 * --para 4 --sourceParallelism 2 --sinkParallelism 2 --logfreq 100000 --words 2000 --delay 0 --sleepFreq 0 --latencyFreq 100000 --local
 */
public class ForwardThroughput {

	public static Logger LOG = LoggerFactory.getLogger(ForwardThroughput.class);

	public static Fields FIELDS = new Fields("string", "time");

	public static class Generator extends BaseRichSpout {

		private final int delay;
		private final boolean withFt;
		private final int latFreq;
		private final ParameterTool pt;
		private SpoutOutputCollector spoutOutputCollector;
		private long id = 0;
		private long time = 0;
		private int sleepFreq;
		private String[] texts;
		private int i = 0;

		public Generator(ParameterTool pt) {
			this.delay = pt.getInt("delay");
			this.withFt = pt.has("ft");
			this.latFreq = pt.getInt("latencyFreq");
			this.sleepFreq = pt.getInt("sleepFreq");
			this.pt = pt;
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
			outputFieldsDeclarer.declare(FIELDS);
		}

		@Override
		public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
			this.spoutOutputCollector = spoutOutputCollector;

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
		public void nextTuple() {
			if(delay > 0) {
				if(id % sleepFreq == 0) {
					try { Thread.sleep(delay); } catch (InterruptedException e) { e.printStackTrace();}
				}
			}
			if(id % latFreq == 0) {
				time = System.currentTimeMillis();
			}

			if(withFt) {
				spoutOutputCollector.emit(new Values(texts[i], this.time), i);
			} else {
				spoutOutputCollector.emit(new Values(texts[i], this.time));
			}

			if(++i == texts.length) {
				i = 0;
			}

			time = 0;
			this.id++;
		}

		@Override
		public void ack(Object msgId) {
		// 	LOG.info("Acked msg "+msgId);
		}

		/**
		 * Resend failed tuple
		 *
		 */
		@Override
		public void fail(Object msgId) {
			Integer id = (Integer)msgId;
			LOG.info("Failed message " + msgId);
			spoutOutputCollector.emit(new Values(texts[id], 0L), id);
		}
	}


	public static class Sink implements IRichBolt {

		private final boolean withFT;
		long received = 0;
		long start = 0;
		ParameterTool pt;
		private OutputCollector collector;
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
		public void execute(Tuple input) {
			try {
				Thread.sleep(50);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			Matcher m = threeDigitAbbr.matcher(input.getString(0));
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

			if(input.getLong(1) != 0) {
				long lat = System.currentTimeMillis() - input.getLong(1);
				LOG.info("Latency {} ms from same machine", lat);
			}

			if(withFT) {
				collector.ack(input);
			}
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
		}

		@Override
		public Map<String, Object> getComponentConfiguration() {
			return null;
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}

		@Override
		public void cleanup() {

		}
	}



	public static void main(String[] args) throws Exception {
		ParameterTool pt = ParameterTool.fromArgs(args);

		int par = pt.getInt("para");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("source0", new Generator(pt), pt.getInt("sourceParallelism"));

		builder.setBolt("sink", new Sink(pt), pt.getInt("sinkParallelism")).noneGrouping("source0");


		Config conf = new Config();
		conf.setDebug(false);

		conf.setMaxSpoutPending(pt.getInt("maxPending", 1000));
		//System.exit(1);

		if (!pt.has("local")) {
			conf.setNumWorkers(par);

			StormSubmitter.submitTopologyWithProgressBar("forward-throughput-"+pt.get("name", "no_name"), conf, builder.createTopology());
		}
		else {
			conf.setMaxTaskParallelism(par);

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("forward-throughput", conf, builder.createTopology());

			Thread.sleep(300000);

			cluster.shutdown();
		}

	}
}
