package storm.starter.trident;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.metric.LoggingMetricsConsumer;

public class TridentWordCount {
    public static final Logger LOG = LoggerFactory.getLogger(TridentWordCount.class);

    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            // System.out.println("Split execute called");
//            LOG.info("splitting: " + tuple.toString());

            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }
    public static class Echo extends BaseFunction {
    	@Override
    	public void execute(TridentTuple tuple, TridentCollector collector) {
    		// System.out.println("Split execute called");
    		LOG.info("echo: " + tuple.toString());
    		
    		
    	}
    }

    public static StormTopology buildTopology(LocalDRPC drpc) {
        InstrumentedFixedBatchSpout spout = new InstrumentedFixedBatchSpout(
                new Fields("sentence"), 10, new Values(
                        "the cow jumped over the moon"), new Values(
                        "the man went to the store and bought some candy"),
                new Values("four score and seven years ago"), new Values(
                        "how many apples can you eat"), new Values(
                        "to be or not to be the person"));
        spout.setCycle(true);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology
                .newStream("spout1", spout)
                .parallelismHint(1)
                .each(new Fields("sentence"), new Split(), new Fields("word"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new InstrumentedMemoryMapState.Factory(),
                        new Count(), new Fields("count"))
                 .newValuesStream().each(new Fields("count","word"), new Echo(), new Fields("abc"))
                  .groupBy(new Fields("abc"))
                .persistentAggregate(new InstrumentedMemoryMapState.Factory(),
                        new Count(), new Fields("countabc"))
;

        // topology.newDRPCStream("words", drpc)
        // .each(new Fields("args"), new Split(), new Fields("word"))
        // .groupBy(new Fields("word"))
        // .stateQuery(wordCounts, new Fields("word"), new MapGet(), new
        // Fields("count"))
        // .each(new Fields("count"), new FilterNull())
        // .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
        // ;
        // topology.newDRPCStream("words", drpc)
        // .each(new Fields("args"), new Split(), new Fields("word"))
        // .groupBy(new Fields("word"))
        // .stateQuery(wordCounts, new Fields("word"), new MapGet(), new
        // Fields("count"))
        // .each(new Fields("count"), new FilterNull())
        // .aggregate(new Fields("count"), new Sum(), new Fields("sum"))
        // ;
        return topology.build();
    }

    public static void main(String[] args) throws Exception {

        Config conf = new Config();
        conf.setMaxSpoutPending(3);
        conf.setDebug(false);
        conf.put(conf.TOPOLOGY_BUILTIN_METRICS_BUCKET_SIZE_SECS, 5);
        conf.registerMetricsConsumer(LoggingMetricsConsumer.class);

        conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 1500);

        System.out.println(conf);

        if (args.length == 0) {
            LocalDRPC drpc = new LocalDRPC();
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(drpc));
            // for(int i=0; i<100; i++) {
            // System.out.println("DRPC RESULT: " + drpc.execute("words",
            // "cat the dog jumped"));
            // Thread.sleep(1000);
            // }
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(null));
        }
    }
}
