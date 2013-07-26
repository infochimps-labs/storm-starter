package com.infochimps.storm.testrig.topology;

import java.util.Arrays;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaConfig.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.TridentKafkaConfig;
import storm.starter.trident.InstrumentedMemoryMapState;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class TridentKafkaExample {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            String sentence = tuple.getString(0);

            // System.out.println("Split execute called on : " + sentence +
            // tuple);

            if (sentence.startsWith("1")) {
                System.out.println("Sleeping");
                try {
                    Thread.sleep(1000000);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));

            }
        }
    }

    public static void main(String[] args) throws Exception, InvalidTopologyException {
        String[] servers = { "localhost" };// list of Kafk a

        int partitionsPerHost = 3;

        BrokerHosts brokers = KafkaConfig.StaticHosts.fromHostString(Arrays
                .asList(servers), partitionsPerHost);

        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(brokers, "testrig3");

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        spoutConfig.fetchSizeBytes = 1024 * 1024 * 500;

        spoutConfig.forceStartOffsetTime(-2);

        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(spoutConfig);
        // OpaqueTridentKafkaSpout kafkaSpout = new
        // OpaqueTridentKafkaSpout(spoutConfig);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology
                .newStream("spout1", kafkaSpout)
                .parallelismHint(3)
                .each(new Fields("str"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new InstrumentedMemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(1);

        Config conf = new Config();
        conf.setMessageTimeoutSecs(10);
        // conf.setMaxSpoutPending(3);
        if (args.length == 0) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, topology.build());
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, topology.build());
        }
    }
}
