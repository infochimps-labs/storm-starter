package storm.starter.trident;

import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.tuple.TridentTuple;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import com.infochimps.storm.testrig.util.Delayer;

public class DelayedWordCount {
    public static class Split extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            // System.out.println("Split execute called");

            String sentence = tuple.getString(0);
            for (String word : sentence.split(" ")) {
                collector.emit(new Values(word));
            }
        }
    }

    public static class Sleep extends BaseFunction {
        long _time = 0L;

        public Sleep(long time) {
            _time = time;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            Delayer.sleep(_time);
            collector.emit(new Values(tuple.getString(0)));
        }
    }

    public static class WasteCPU extends BaseFunction {
        long _noOfExecutions = 0L;

        public WasteCPU(long noOfExecutions) {
            this._noOfExecutions = noOfExecutions;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            Delayer.wasteCPU(_noOfExecutions);

            collector.emit(new Values(tuple.getString(0)));

        }

        public static StormTopology buildTopology(String[] args) {

            long sleeptime = Long.parseLong(args[0]);
            long noOfExecutions = Long.parseLong(args[1]);

            InstrumentedFixedBatchSpout spout = new InstrumentedFixedBatchSpout(
                    new Fields("sentence"), 100, new Values(
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
                    .each(new Fields("sentence"), new WasteCPU(noOfExecutions),
                            new Fields("sentence1"))
                    .each(new Fields("sentence1"), new Sleep(sleeptime),
                            new Fields("sentence2"))
                    .each(new Fields("sentence2"), new Split(),
                            new Fields("word"))
                    .groupBy(new Fields("word"))
                    .persistentAggregate(
                            new InstrumentedMemoryMapState.Factory(),
                            new Count(), new Fields("count"))
                    .parallelismHint(1);

            return topology.build();
        }

        public static void main(String[] args) throws Exception {

            String[] dummy = { "10", "0" };
            args = dummy;

            Config conf = new Config();
            conf.setMaxSpoutPending(900);
            conf.put(Config.TOPOLOGY_SLEEP_SPOUT_WAIT_STRATEGY_TIME_MS, 10);
            conf.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 10);
            conf.setMessageTimeoutSecs(3600);
            if (args.length == 2) {
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("wordCounter", conf, buildTopology(args));
                Utils.sleep(10000);
                cluster.killTopology("wordCluster");
            } else {
                conf.setNumWorkers(3);
                StormSubmitter.submitTopology(args[2], conf,
                        buildTopology(args));

            }
        }
    }
}
