package experiments;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.apache.flink.api.java.utils.ParameterTool;
import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import storm.trident.testing.FixedBatchSpout;

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.UUID;

public class KafkaThroughput {

	public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, UnknownHostException, InterruptedException {
		final ParameterTool pt = ParameterTool.fromArgs(args);

		TopologyBuilder builder = new TopologyBuilder();
		BrokerHosts hosts = new ZkHosts(pt.getRequired("zookeeper"));
		SpoutConfig spoutConfig = new SpoutConfig(hosts, pt.getRequired("topic"), "/" + pt.getRequired("topic"), UUID.randomUUID().toString());
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		builder.setSpout("source", kafkaSpout, pt.getInt("sourceParallelism"));

		builder.setBolt("sink", new Throughput.Sink(pt), pt.getInt("sinkParallelism")).noneGrouping("source");

		Config conf = new Config();
		conf.setDebug(false);

		if (!pt.has("local")) {
			conf.setNumWorkers(pt.getInt("par", 2));

			StormSubmitter.submitTopologyWithProgressBar("kafka-spout-"+pt.get("name", "no_name"), conf, builder.createTopology());
		} else {
			conf.setMaxTaskParallelism(pt.getInt("par", 2));

			LocalCluster cluster = new LocalCluster();
			cluster.submitTopology("kafka-spout", conf, builder.createTopology());

			Thread.sleep(300000);

			cluster.shutdown();
		}
	}
}
