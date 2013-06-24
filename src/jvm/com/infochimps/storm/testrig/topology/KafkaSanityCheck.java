package com.infochimps.storm.testrig.topology;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaConfig.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.ImmutableList;

public class KafkaSanityCheck {

	public static class PassThroughBolt extends BaseRichBolt {

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			// TODO Auto-generated method stub

		}

		@Override
		public void execute(Tuple input) {
			System.out.println(input.getString(0));
		}

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			// TODO Auto-generated method stub

		}

	}

	public static void main(String[] args) {
		TopologyBuilder builder = new TopologyBuilder();
		List<String> hosts = new ArrayList();
		hosts.add("localhost");
		int partitionsPerHost = 1;

		BrokerHosts brokers = KafkaConfig.StaticHosts.fromHostString(hosts,
				partitionsPerHost);
		SpoutConfig spoutConfig = new SpoutConfig(brokers, "testrig", "/foo",
				"foo");
		spoutConfig.zkServers = ImmutableList.of("localhost");
		spoutConfig.zkPort = 2181;
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

		// spoutConfig.scheme = new StringScheme();
		builder.setSpout("spout", new KafkaSpout(spoutConfig));

		PassThroughBolt bolt = new PassThroughBolt();
		builder.setBolt("loader", bolt).shuffleGrouping("spout");
		Config conf = new Config();
		conf.setMaxSpoutPending(5);
		conf.setMessageTimeoutSecs(30);
		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("wordCounter", conf, builder.createTopology());

	}

}
