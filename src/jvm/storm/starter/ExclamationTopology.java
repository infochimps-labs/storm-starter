package storm.starter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

import storm.starter.spout.RandomSentenceSpout;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import backtype.storm.metric.LoggingMetricsConsumer;

/**
 * This is a basic example of a Storm topology.
 */
public class ExclamationTopology {
    public static final Logger LOG = LoggerFactory.getLogger(ExclamationTopology.class);

    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
            // Utils.sleep(5);
            LOG.info("execute: " + tuple.toString());
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("word", new RandomSentenceSpout(), 1);
        builder.setBolt("exclaim1", new ExclamationBolt(), 1)
            .shuffleGrouping("word");
        builder.setBolt("exclaim2", new ExclamationBolt(), 2)
            .shuffleGrouping("exclaim1");

        Config conf = new Config();
        conf.setDebug(false);
        conf.put(conf.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);
        System.out.println(conf);

        for (Map.Entry e : conf.entrySet()) {
            System.out.println(e.getKey() + " = " + e.getValue());

        }

        if (args != null && args.length > 0) {
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology(args[0], conf,
                    builder.createTopology());
        } else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(2000000);
            // Utils.sleep(10000000);
            // Utils.sleep(10000000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

}
