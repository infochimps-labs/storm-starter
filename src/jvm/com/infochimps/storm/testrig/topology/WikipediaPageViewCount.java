package com.infochimps.storm.testrig.topology;

import java.util.Arrays;

import storm.kafka.KafkaConfig;
import storm.kafka.KafkaConfig.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.OpaqueTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
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
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class WikipediaPageViewCount {

    public static class URLSlugEmitter extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String sentence = tuple.getString(0);
            String[] words = sentence.split(" ");
            // Emit last 4 chars of the URL
            System.out.println(words[1].substring(words[1].length() - 4,
                    words[1].length()));
            System.out.println("URLSlugEmitter : emit");
            collector.emit(new Values(words[0]));
        }
    }

    public static class WasteCPU extends BaseFunction {
        long _noOfExecutions = 0L;

        public WasteCPU(long noOfExecution) {
            this._noOfExecutions = noOfExecution;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            System.out.println("Wasting CPU's");
            int N = 6000;
            double[] Crb = new double[N + 7];
            double[] Cib = new double[N + 7];
            double invN = 2.0 / N;
            for (int i = 0; i < N; i++) {
                Cib[i] = i * invN - 1.0;
                Crb[i] = i * invN - 1.5;
            }
            for (int j = 0; j < _noOfExecutions; j++) {
                for (int i = 0; i < N; i++) {
                    getByte(i, i, Crb, Cib);
                }
            }
            collector.emit(new Values(tuple.getString(0)));

        }

        static int getByte(int x, int y, double[] Crb, double[] Cib) {
            int res = 0;
            for (int i = 0; i < 8; i += 2) {
                double Zr1 = Crb[x + i];
                double Zi1 = Cib[y];

                double Zr2 = Crb[x + i + 1];
                double Zi2 = Cib[y];

                int b = 0;
                int j = 49;
                do {
                    double nZr1 = Zr1 * Zr1 - Zi1 * Zi1 + Crb[x + i];
                    double nZi1 = Zr1 * Zi1 + Zr1 * Zi1 + Cib[y];
                    Zr1 = nZr1;
                    Zi1 = nZi1;

                    double nZr2 = Zr2 * Zr2 - Zi2 * Zi2 + Crb[x + i + 1];
                    double nZi2 = Zr2 * Zi2 + Zr2 * Zi2 + Cib[y];
                    Zr2 = nZr2;
                    Zi2 = nZi2;

                    if (Zr1 * Zr1 + Zi1 * Zi1 > 4) {
                        b |= 2;
                        if (b == 3) {
                            break;
                        }
                    }
                    if (Zr2 * Zr2 + Zi2 * Zi2 > 4) {
                        b |= 1;
                        if (b == 3) {
                            break;
                        }
                    }
                } while (--j > 0);
                res = (res << 2) + b;
            }
            return res ^ -1;
        }

    }

    public static class Sleep extends BaseFunction {
        long _time = 0L;

        public Sleep(long time) {
            _time = time;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            System.out.println("Sleeping");

            Utils.sleep(_time);
            collector.emit(new Values(tuple.getString(0)));
        }
    }

    @SuppressWarnings({ "unused" })
    public StormTopology buildTopology(String args[]) {

        long sleeptime = Long.parseLong(args[0]);
        long noOfExecutions = Long.parseLong(args[1]);

        String[] servers = { "localhost" };// list of Kafka
        // brokers

        int partitionsPerHost = 1;

        BrokerHosts brokers = KafkaConfig.StaticHosts.fromHostString(
                Arrays.asList(servers), partitionsPerHost);

        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(brokers,
                "testrig3");

        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        // spoutConfig.scheme = new StringScheme();

        spoutConfig.fetchSizeBytes = 1024 * 1024 * 5;

        spoutConfig.forceStartOffsetTime(-2);

        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(
                spoutConfig);

        TridentTopology topology = new TridentTopology();
        TridentState wordCounts = topology
                .newStream("testrig-a", kafkaSpout)
                .parallelismHint(1)
                .each(new Fields("str"), new URLSlugEmitter(),
                        new Fields("word"))
                // .each(new Fields("word"), new WasteCPU(noOfExecutions),
                // new Fields("word1"))
                // .each(new Fields("word1"), new Sleep(sleeptime),
                // new Fields("word2"))
                .groupBy(new Fields("word"))
                .persistentAggregate(new InstrumentedMemoryMapState.Factory(),
                        new Count(), new Fields("count")).parallelismHint(1);

        return topology.build();
    }

    public void run(String[] args) throws Exception {
        Config conf = new Config();
        conf.setMaxSpoutPending(5);
        conf.setMessageTimeoutSecs(3000);
        if (args.length >= 2) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("wordCounter", conf, buildTopology(args));
            Utils.sleep(10000);
            cluster.killTopology("wordCluster");
        } else {
            conf.setNumWorkers(3);
            StormSubmitter.submitTopology(args[0], conf, buildTopology(args));
        }
    }

    public static void main(String[] args) throws Exception {
        String[] dummy = { "20000000", "9000" };
        args = dummy;
        WikipediaPageViewCount wc = new WikipediaPageViewCount();
        wc.run(args);
    }
}
